"""Endpoint-only Binance USDⓈ-M / Hyperliquid basis attribution."""

from decimal import Decimal, InvalidOperation


def _decimal(value):
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() else None


def _positive(value):
    number = _decimal(value)
    return number if number is not None and number > 0 else None


def make_snapshot(binance_positions, hyperliquid, bn_at, hl_at, sampled_at, symbol_map):
    if isinstance(binance_positions, dict):
        binance_positions = binance_positions.get("data")
    hl_positions = hyperliquid.get("positions") if hyperliquid else None
    if not isinstance(binance_positions, list) or not isinstance(hl_positions, list) or not bn_at or not hl_at:
        return None
    result = {"sampledAt": sampled_at.isoformat(), "binanceAt": bn_at.isoformat(),
              "hyperliquidAt": hl_at.isoformat(), "hyperliquidMode": hyperliquid.get("mode"),
              "hyperliquidStateTimesMs": hyperliquid.get("positionStateTimesMs", {}),
              "binance": [], "hyperliquid": []}
    for row in binance_positions:
        quantity = _decimal(row.get("positionAmt"))
        if quantity == 0:
            continue
        symbol = str(row.get("symbol", "")).upper()
        mark = _positive(row.get("markPrice"))
        result["binance"].append({
            "symbol": symbol, "positionSide": row.get("positionSide"),
            "quantity": str(quantity) if quantity is not None else None,
            "markNative": str(mark) if mark is not None else None,
            "markU": str(mark) if mark is not None and symbol.endswith("USDT") else None,
            "markSource": "markPrice",
        })
    for row in hl_positions:
        quantity = _decimal(row.get("szi"))
        if quantity == 0:
            continue
        coin, dex = str(row.get("coin", "")), row.get("dex", "")
        symbol = str(symbol_map.get(coin) or coin.rsplit(":", 1)[-1]).upper()
        entry, upl = _positive(row.get("entryPx")), _decimal(row.get("unrealizedPnl"))
        inferred = entry + upl / quantity if entry is not None and upl is not None and quantity else None
        collateral = hyperliquid.get("positionCollateral", {}).get(dex)
        # First version only values USDC collateral, using the monitor's configured rate.
        rate = _positive(hyperliquid.get("usdcToUsdt")) if collateral == "USDC" else None
        state_ms = hyperliquid.get("positionStateTimesMs", {}).get(dex)
        try:
            skew = abs(bn_at.timestamp() - int(state_ms) / 1000)
        except (TypeError, ValueError, OverflowError):
            skew = None
        mark = inferred * rate if inferred is not None and inferred > 0 and rate is not None else None
        result["hyperliquid"].append({
            "symbol": symbol, "nativeSymbol": coin, "dex": dex, "collateral": collateral,
            "quantity": str(quantity) if quantity is not None else None,
            "markNative": str(inferred) if inferred is not None and inferred > 0 else None,
            "markU": str(mark) if mark is not None else None, "markSource": "entryPx + unrealizedPnl / szi",
            "stateTimeMs": state_ms, "sampleSkewSeconds": skew,
        })
    return result


def _group(rows, aliases):
    result = {}
    for row in rows:
        symbol = row["symbol"]
        base = next((symbol[:-len(quote)] for quote in ("USDT", "USDC")
                     if symbol.endswith(quote) and len(symbol) > len(quote)), symbol)
        result.setdefault(aliases.get(base, base) or "?", []).append(row)
    return result


def _endpoint(bn, hl):
    if len(bn) > 1 or len(hl) > 1:
        return None, "同标的存在多笔持仓，未自动合并"
    bq = _decimal(bn[0]["quantity"]) if bn else Decimal(0)
    hq = _decimal(hl[0]["quantity"]) if hl else Decimal(0)
    if bq is None or hq is None:
        return None, "持仓数量缺失"
    bm = _positive(bn[0]["markU"]) if bn else None
    hm = _positive(hl[0]["markU"]) if hl else None
    hedge = min(abs(bq), abs(hq)).copy_sign(bq) if bq * hq < 0 else Decimal(0)
    spread = bm - hm if bm is not None and hm is not None else None
    error = ""
    if hedge:
        skew = hl[0]["sampleSkewSeconds"]
        if skew is None or skew > 30:
            error = "双边采样时间缺失或相差超过 30 秒"
        elif spread is None:
            error = "双边标记价或折算价格不可用"
    value = None if error else hedge * spread if hedge else Decimal(0)
    return (bq, bm, hq, hm, hedge, spread, bq + hq, value), error


def basis_rows(baseline, latest, aliases):
    if not baseline or not latest:
        return []
    groups = [_group(snapshot[leg], aliases) for snapshot in (baseline, latest)
              for leg in ("binance", "hyperliquid")]
    rows = []
    for symbol in sorted(set().union(*groups)):
        legs = [group.get(symbol, []) for group in groups]
        start, start_error = _endpoint(*legs[:2])
        end, end_error = _endpoint(*legs[2:])
        change = None
        if start and end and start[-1] is not None and end[-1] is not None:
            change = end[-1] - start[-1]
        rows.append((symbol, legs, start, end, change, start_error or end_error))
    return rows


def basis_total(baseline, latest, aliases):
    """Return a complete total; missing samples or partial coverage are not zero."""
    for snapshot in (baseline, latest):
        if not isinstance(snapshot, dict) or any(
            not isinstance(snapshot.get(leg), list) for leg in ("binance", "hyperliquid")
        ):
            return None
    changes = [row[4] for row in basis_rows(baseline, latest, aliases)]
    if any(change is None for change in changes):
        return None
    return sum(changes, Decimal(0))

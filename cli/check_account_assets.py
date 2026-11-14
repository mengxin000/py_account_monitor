"""Read-only equity cross-check for Binance USDⓈ-M and Hyperliquid.

Examples (PowerShell):
    uv run python -m cli.check_account_assets --config config/account_mfx.local.json --hyperliquid-address 0x...

The Binance API key needs USDⓈ-M account read permission. This script does not
place/cancel orders or create/renew user streams.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
from pathlib import Path
from typing import Any

# Also support direct execution as `python cli/check_account_assets.py`.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from ..connections.binance.base import BinanceCredentials
    from ..connections.binance.rest import BinanceRestClient
    from ..connections.hyperliquid.rest import HyperliquidRestClient
    from ..config.credentials import credential_pair
    from ..config.legs import resolve_legs
except ImportError:  # `python -m cli.check_account_assets` from project root
    from connections.binance.base import BinanceCredentials
    from connections.binance.rest import BinanceRestClient
    from connections.hyperliquid.rest import HyperliquidRestClient
    from config.credentials import credential_pair
    from config.legs import resolve_legs


def load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"expected a JSON object in {path}")
    return data


def binance_config(data: dict[str, Any], config_path: Path) -> tuple[BinanceCredentials, str]:
    usdm = data.get("usdm") or {}
    if not isinstance(usdm, dict):
        raise ValueError("config.usdm must be an object")
    futures = data.get("futures") or {}
    credential_values: dict[str, Any] = {}
    credential_file = data.get("credentials_file")
    if credential_file:
        referenced_path = (config_path.parent / str(credential_file)).resolve()
        credential_values = load_json(referenced_path)
    env_key = os.getenv("BINANCE_API_KEY")
    env_secret = os.getenv("BINANCE_SECRET_KEY")
    if bool(env_key) != bool(env_secret):
        raise ValueError("set both BINANCE_API_KEY and BINANCE_SECRET_KEY, or neither")
    if env_key and env_secret:
        pair = (env_key, env_secret, os.getenv("BINANCE_KEY_TYPE", "auto"))
    else:
        pair = credential_pair([usdm, futures, data, credential_values])
    if not pair[0] or not pair[1]:
        raise ValueError("Binance USDⓈ-M credentials not found in usdm/top-level config or environment")
    creds = BinanceCredentials(
        api_key=pair[0], secret_key=pair[1], key_type=pair[2],
        subaccount_email=data.get("subaccount_email"), label=data.get("account_id"),
    )
    base_url = str(usdm.get("rest_base_url") or usdm.get("base_url") or "https://fapi.binance.com")
    return creds, base_url


def hyper_config(data: dict[str, Any], address_arg: str | None) -> dict[str, Any]:
    config: dict[str, Any] = dict(data.get("hyperliquid") or {})
    legs = data.get("legs")
    if isinstance(legs, dict):
        for leg in legs.values():
            if isinstance(leg, dict) and leg.get("exchange") == "hyperliquid":
                config = {**leg, **config}
                break
    if address_arg:
        config["address"] = address_arg
    address = str(config.get("address") or "").strip()
    if not address:
        raise ValueError("Hyperliquid address missing; pass --hyperliquid-address or configure hyperliquid.address")
    if len(address) != 42 or not address.lower().startswith("0x"):
        raise ValueError("Hyperliquid address should be the 42-character 0x account/subaccount address")
    config["address"] = address
    config["rest_base_url"] = str(config.get("rest_base_url") or "https://api.hyperliquid.xyz")
    return config


def number(value: Any, label: str) -> float:
    result = float(value or 0)
    if not math.isfinite(result):
        raise ValueError(f"non-finite numeric value for {label}")
    return result


async def check_binance(data: dict[str, Any], config_path: Path, proxy: str | None) -> None:
    credentials, base_url = binance_config(data, config_path)
    print("\n=== Binance USD-M ===")
    print(f"account={credentials.label or '-'} endpoint={base_url}/fapi/v3/account key_type={credentials.key_type}")
    async with BinanceRestClient(credentials, base_url=base_url, proxy=proxy) as client:
        account = await client.get("/fapi/v3/account", signed=True)
        positions = await client.get("/fapi/v3/positionRisk", signed=True)
    positions = positions.get("data", positions)
    if not isinstance(positions, list):
        raise ValueError("Binance positionRisk returned an unexpected payload")

    fields = ("totalWalletBalance", "totalUnrealizedProfit", "totalMarginBalance",
              "availableBalance", "totalPositionInitialMargin", "totalOpenOrderInitialMargin",
              "totalMaintMargin")
    for key in fields:
        if key in account:
            print(f"{key}: {account[key]}")
    if "totalWalletBalance" in account and "totalUnrealizedProfit" in account and "totalMarginBalance" in account:
        calculated = number(account["totalWalletBalance"], "wallet") + number(account["totalUnrealizedProfit"], "unrealized pnl")
        reported = number(account["totalMarginBalance"], "margin balance")
        print(f"wallet + account UPL: {calculated:.10f} (API totalMarginBalance diff={reported - calculated:+.10f})")

    assets = account.get("assets") or []
    if assets:
        print("non-zero assets:")
        for item in assets:
            wallet = number(item.get("walletBalance"), "asset wallet")
            upl = number(item.get("unrealizedProfit"), "asset UPL")
            if wallet or upl:
                print(f"  {item.get('asset', '?')}: wallet={item.get('walletBalance', '-')} "
                      f"unrealized={item.get('unrealizedProfit', '-')} "
                      f"margin={item.get('marginBalance', '-')}")

    active = [p for p in positions if isinstance(p, dict) and number(p.get("positionAmt"), "positionAmt") != 0]
    print(f"open positions: {len(active)}")
    summed_upl = 0.0
    for p in active:
        upl = number(p.get("unRealizedProfit"), "position UPL")
        summed_upl += upl
        print(f"  {p.get('symbol')} {p.get('positionSide')} qty={p.get('positionAmt')} "
              f"entry={p.get('entryPrice')} mark={p.get('markPrice')} UPL={p.get('unRealizedProfit')}")
    print(f"positionRisk UPL sum: {summed_upl:.10f}")
    if account.get("totalUnrealizedProfit") is not None:
        print(f"position UPL sum - account UPL: {summed_upl - number(account['totalUnrealizedProfit'], 'account UPL'):+.10f}")


async def check_hyperliquid(config: dict[str, Any], proxy: str | None) -> None:
    from collectors.hyperliquid.normalize import rates

    client = HyperliquidRestClient(config["rest_base_url"], proxy=proxy)
    address = config["address"]
    print("\n=== Hyperliquid ===")
    print(f"address={address} endpoint={config['rest_base_url']}/info")
    try:
        mode, spot, base_meta, dex_list, spot_meta = await asyncio.gather(
            client.info("userAbstraction", user=address),
            client.info("spotClearinghouseState", user=address),
            client.info("meta"), client.info("perpDexs"), client.info("spotMeta"),
        )
        dex_meta: dict[str, dict[str, Any]] = {"": base_meta}
        for dex in dex_list:
            if dex and dex.get("name"):
                dex_name = dex["name"]
                dex_meta[dex_name] = await client.info("meta", dex=dex_name)

        conversion = rates(config)
        tokens = {int(t["index"]): t["name"] for t in spot_meta["tokens"]}
        balances = spot.get("balances")
        if not isinstance(balances, list):
            raise ValueError("spotClearinghouseState returned no balances list")

        unresolved = {str(row["coin"]) for row in balances if number(row.get("total"), "balance") and row["coin"] not in conversion}
        contexts = None
        if unresolved:
            contexts = await client.info("spotMetaAndAssetCtxs")
            for pair, ctx in zip(contexts[0]["universe"], contexts[1]):
                base, quote = (tokens.get(int(i)) for i in pair["tokens"])
                if base and quote in conversion and base not in conversion:
                    try:
                        price = number(ctx.get("markPx"), f"{base} markPx")
                    except (TypeError, ValueError):
                        continue
                    if price > 0:
                        conversion[base] = price * conversion[quote]

        unresolved = sorted(coin for coin in unresolved if coin not in conversion)
        if unresolved:
            raise ValueError("cannot value these non-zero spot balances; add token_rates in config: " + ", ".join(unresolved))

        spot_value = 0.0
        print(f"account mode: {mode}; USDC to USDT={conversion.get('USDC', 1.0)}")
        print("balances from spotClearinghouseState (unified account; total includes hold):")
        for row in balances:
            coin = str(row["coin"])
            total = number(row.get("total"), f"{coin} total")
            hold = number(row.get("hold"), f"{coin} hold")
            if total:
                rate = conversion[coin]
                value = total * rate
                spot_value += value
                print(f"  {coin}: total={total:.10f} hold={hold:.10f} rate={rate:.10f} value={value:.10f} U")

        states = await asyncio.gather(*(
            client.info("clearinghouseState", user=address, dex=dex) for dex in dex_meta
        ))
        positions: list[tuple[str, dict[str, Any]]] = []
        dex_account_values: dict[str, float] = {}
        dex_diagnostics: dict[str, dict[str, Any]] = {}
        upl_total = 0.0
        for dex, state in zip(dex_meta, states):
            margin = state.get("marginSummary") or {}
            cross_margin = state.get("crossMarginSummary") or {}
            dex_diagnostics[dex] = {
                "accountValue": margin.get("accountValue"),
                "crossAccountValue": cross_margin.get("accountValue"),
                "crossMaintenanceMarginUsed": state.get("crossMaintenanceMarginUsed"),
                "time": state.get("time"),
            }
            collateral_token = tokens.get(int(dex_meta[dex].get("collateralToken", 0)))
            collateral_rate = conversion.get(collateral_token or "")
            if mode == "default" and margin.get("accountValue") is not None:
                if collateral_rate is None:
                    raise ValueError(f"missing conversion rate for {collateral_token} collateral on DEX {dex or 'main'}")
                dex_account_values[dex] = number(margin["accountValue"], "accountValue") * collateral_rate
            for row in state.get("assetPositions", []):
                position = row.get("position", {})
                if number(position.get("szi"), "position size"):
                    positions.append((dex, position))
                if position.get("unrealizedPnl") is not None:
                    if collateral_rate is None:
                        raise ValueError(f"missing conversion rate for {collateral_token} collateral on DEX {dex or 'main'}")
                    upl_total += number(position.get("unrealizedPnl"), "position UPL") * collateral_rate

        for dex, position in positions:
            print(f"  perp {dex or 'main'}:{position.get('coin')} qty={position.get('szi')} "
                  f"entry={position.get('entryPx')} value={position.get('positionValue')} "
                  f"UPL={position.get('unrealizedPnl')} {tokens.get(int(dex_meta[dex].get('collateralToken', 0)), 'collateral')}")

        if mode == "unifiedAccount":
            equity = spot_value
            print(f"unified balance value (all balances from spotClearinghouseState): {spot_value:.10f} U")
            print(f"perp UPL (display only; not added again): {upl_total:+.10f} U")
            print(f"calculated Hyperliquid equity: {equity:.10f} U")
            print("per-DEX accountValue diagnostics (not summed; unified collateral is shared):")
            for dex, values in dex_diagnostics.items():
                print(f"  {dex or 'main'}: accountValue={values['accountValue']} "
                      f"crossAccountValue={values['crossAccountValue']} "
                      f"crossMaintenance={values['crossMaintenanceMarginUsed']} stateTime={values['time']}")
        elif mode == "default":
            perp_value = sum(dex_account_values.values())
            equity = spot_value + perp_value
            print(f"spot value: {spot_value:.10f} U")
            print("per-DEX accountValue (already includes that DEX's UPL):")
            for dex, value in dex_account_values.items():
                print(f"  {dex or 'main'}: {value:.10f} U")
            print(f"calculated Hyperliquid equity: {equity:.10f} U")
        else:
            raise ValueError(f"unsupported account abstraction mode: {mode}")
    finally:
        await client.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only Binance USD-M and Hyperliquid equity cross-check")
    parser.add_argument("--config", type=Path, default=Path(__file__).parents[1] / "config" / "account_mfx.local.json",
                        help="account-local JSON containing Binance usdm credentials and optionally Hyperliquid config")
    parser.add_argument("--hyperliquid-address", help="Hyperliquid actual account/subaccount 0x address; overrides config")
    parser.add_argument("--proxy", help="optional HTTP(S) proxy URL; otherwise aiohttp trust_env behavior is used")
    return parser


async def run_audit(args: argparse.Namespace) -> None:
    data = resolve_legs(load_json(args.config))
    print(f"asset audit account={data.get('account_id', args.config.stem)} config={args.config}")
    print("Read-only: Binance GET account/positionRisk; Hyperliquid info queries only.")
    errors: list[str] = []
    try:
        await check_binance(data, args.config.resolve(), args.proxy)
    except Exception as exc:
        errors.append(f"Binance USDⓈ-M: {type(exc).__name__}: {exc}")
        print(f"\nBinance USDⓈ-M ERROR: {type(exc).__name__}: {exc}")
    try:
        await check_hyperliquid(hyper_config(data, args.hyperliquid_address), args.proxy)
    except Exception as exc:
        errors.append(f"Hyperliquid: {type(exc).__name__}: {exc}")
        print(f"\nHyperliquid ERROR: {type(exc).__name__}: {exc}")
    if errors:
        raise SystemExit("\nAudit incomplete:\n- " + "\n- ".join(errors))


def main() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    args = build_parser().parse_args()
    asyncio.run(run_audit(args))


if __name__ == "__main__":
    main()

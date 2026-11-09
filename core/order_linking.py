"""Order association policies; never decide economic P&L here."""
import re


def decimal_prefixes(cloid):
    if not re.fullmatch(r"0x[0-9a-fA-F]{32}", cloid):
        return ()
    digits = re.match(r"[0-9]*", cloid[2:]).group()
    return tuple(digits[:n] for n in range(1, len(digits) + 1))


def is_matching_order(current_id, system_id, match_system_id, match_client_id):
    if "exposure" in current_id or "exposure" in match_client_id:
        return False
    return bool((match_system_id and match_system_id in current_id)
                or (system_id and system_id in match_client_id)
                or (match_client_id and match_client_id in current_id)
                or (current_id and current_id in match_client_id))


def orders_linked(a, b):
    if a.side == b.side or (a.account_id and b.account_id and a.account_id != b.account_id):
        return False
    if "exposure" in a.id.lower() or "exposure" in b.id.lower():
        return False
    if a.exchange == b.exchange == "binance":
        return is_matching_order(a.id, a.system_id, b.system_id, b.id)
    if {a.exchange, b.exchange} == {"binance", "hyperliquid"}:
        bn, hl = (a, b) if a.exchange == "binance" else (b, a)
        # Decimal digits are embedded literally; never convert cloid from hex.
        return bool(re.fullmatch(r"[0-9]+", bn.system_id)
                    and re.fullmatch(r"0x[0-9a-fA-F]{32}", hl.id)
                    and hl.id[2:2 + len(bn.system_id)] == bn.system_id)
    return False

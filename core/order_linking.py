"""Order association policies; never decide economic P&L here."""
import re


def binance_sid_from_cloid(cloid):
    """Extract the literal decimal SID from 0xb<SID>b<hex suffix>."""
    if not re.fullmatch(r"0x[0-9a-fA-F]{32}", cloid):
        return None
    match = re.fullmatch(r"0xb([0-9]+)b[0-9a-fA-F]+", cloid)
    return match.group(1) if match else None


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
                    and binance_sid_from_cloid(hl.id) == bn.system_id)
    return False

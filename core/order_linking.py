"""Order association policies; never decide economic P&L here."""
import re
from types import MappingProxyType


def reference_sid_from_cloid(cloid):
    """Extract the counterpart's literal decimal SID from 0xb<SID>b<suffix>."""
    if not re.fullmatch(r"0x[0-9a-fA-F]{32}", cloid):
        return None
    match = re.fullmatch(r"0xb([0-9]+)b[0-9a-fA-F]+", cloid)
    return match.group(1) if match else None


# Kept for existing imports; the embedded SID may belong to OKX as well.
binance_sid_from_cloid = reference_sid_from_cloid


def is_matching_order(current_id, system_id, match_system_id, match_client_id):
    if "exposure" in current_id or "exposure" in match_client_id:
        return False
    return bool((match_system_id and match_system_id in current_id)
                or (system_id and system_id in match_client_id)
                or (match_client_id and match_client_id in current_id)
                or (current_id and current_id in match_client_id))


def contains_ids(a, b):
    return is_matching_order(a.id, a.system_id, b.system_id, b.id)


def reference_sid(order):
    if order.exchange == "hyperliquid":
        # Legacy JSONL lacks reference_sid. Decode without rewriting journals.
        decoded = reference_sid_from_cloid(order.id) or ""
        if order.reference_sid and order.reference_sid != decoded:
            return ""  # Inconsistent normalized identity must never match.
        return decoded
    return order.reference_sid


def references_sid(a, b):
    return bool((reference_sid(a) and reference_sid(a) == b.system_id)
                or (reference_sid(b) and reference_sid(b) == a.system_id))


# Unordered pairs: each symmetric cell is declared only once. Unknown pairs
# are not allowed to silently fall back to substring matching.
RULE_TABLE = MappingProxyType({
    frozenset(("binance",)): contains_ids,
    frozenset(("binance", "okx")): contains_ids,
    frozenset(("okx",)): contains_ids,
    frozenset(("binance", "hyperliquid")): references_sid,
    frozenset(("okx", "hyperliquid")): references_sid,
})

RULE_NAMES = {"contains_ids": contains_ids, "references_sid": references_sid}


def compile_rule_table(mapping=None):
    """Compile an account-local matrix. Missing config preserves legacy defaults.

    An explicit matrix replaces defaults; omitted cells cannot pair. Symmetric
    cells may be written once, or twice with the same rule. Never use eval.
    """
    if mapping is None:
        return RULE_TABLE
    if not isinstance(mapping, dict):
        raise ValueError("matching_rules must be a nested exchange/rule object")
    table = {}
    for exchange, row in mapping.items():
        if not isinstance(exchange, str) or not exchange or not isinstance(row, dict):
            raise ValueError("invalid matching_rules row")
        for other, name in row.items():
            if not isinstance(other, str) or not other or name not in (*RULE_NAMES, "disabled"):
                raise ValueError("unknown matching_rules exchange or rule")
            key = frozenset((exchange, other))
            rule = RULE_NAMES.get(name)
            if key in table and table[key] is not rule:
                raise ValueError(f"conflicting symmetric matching_rules: {exchange}/{other}")
            table[key] = rule
    return MappingProxyType(table)


def orders_linked(a, b, rule_table=None):
    if a.side == b.side or (a.account_id and b.account_id and a.account_id != b.account_id):
        return False
    if "exposure" in a.id.lower() or "exposure" in b.id.lower():
        return False
    rule = (RULE_TABLE if rule_table is None else rule_table).get(frozenset((a.exchange, b.exchange)))
    return bool(rule and rule(a, b))


def order_identity(order):
    """Logical order, not a fill: keep venue/scope/symbol, omit execution_id."""
    return (order.exchange, order.account_id, order.account_scope,
            order.market_type, order.symbol, order.system_id or order.id)


class AmbiguousOrderLink(ValueError):
    pass


def validate_candidates(current, candidates, rule_table=None):
    identities = {order_identity(order) for order in candidates}
    if len(identities) <= 1:
        return
    # Multiple hedge orders can explicitly reference ONE original order. This
    # is not ambiguous, unlike one substring accidentally selecting two SIDs.
    def targets_current(order):
        rule = (RULE_TABLE if rule_table is None else rule_table).get(frozenset((current.exchange, order.exchange)))
        if rule is references_sid:
            return bool(current.system_id and reference_sid(order) == current.system_id)
        return bool(rule is contains_ids and current.system_id and current.system_id in order.id
                    and not (order.system_id and order.system_id in current.id))
    if all(targets_current(order) for order in candidates):
        return
    raise AmbiguousOrderLink(
        f"ambiguous order association current={order_identity(current)!r} "
        f"candidates={sorted(identities)!r}; replay stopped before quantity allocation")

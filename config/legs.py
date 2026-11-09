"""Translate explicit legs into the existing Binance modes plus an HL adapter."""
import math
import re
from .credentials import credential_pair


def resolve_legs(values):
    if "legs" not in values:
        return values
    legs = values["legs"]
    if not isinstance(legs, dict) or not 1 <= len(legs) <= 2:
        raise ValueError("legs must contain one or two named legs")
    result = dict(values)
    result.update(spot={"mode": "none"}, futures={"mode": "none"}, usdm={}, hyperliquid={"enabled": False})
    used = set()
    pm_pair = None
    for name, leg in legs.items():
        if not isinstance(leg, dict):
            raise ValueError(f"leg {name} must be an object")
        exchange, market = leg.get("exchange"), leg.get("market")
        if exchange == "hyperliquid" and market == "perp":
            if "hyperliquid" in used:
                raise ValueError("only one Hyperliquid account is supported per monitoring account")
            result["hyperliquid"] = {**leg, "enabled": True, "leg": name}
            used.add("hyperliquid")
        elif exchange == "binance" and market in {"spot", "pm_margin", "usdm", "pm_um"}:
            kind = "spot" if market in {"spot", "pm_margin"} else "futures"
            if kind in used:
                raise ValueError(f"duplicate Binance {kind} leg")
            settings = {**leg.get("credentials", {}), **leg, "mode": market}
            result[kind] = settings
            if market in {"pm_margin", "pm_um"}:
                pair = credential_pair([settings])
                if pair[0] is not None:
                    if pm_pair is not None:
                        if pair[:2] != pm_pair[:2] or (pair[2] != pm_pair[2] and "auto" not in (pair[2], pm_pair[2])):
                            raise ValueError("PM margin and PM futures share one connection; their credentials must agree")
                        if pair[2] == "auto":
                            pair = (pair[0], pair[1], pm_pair[2])
                    pm_pair = pair
                    # Explicit legs override legacy top-level credentials.
                    for key in ("api_key_env", "secret_key_env", "key_type_env", "apiKey", "secKey"):
                        result.pop(key, None)
                    result.update(api_key=pair[0], secret_key=pair[1], key_type=pair[2])
            if market == "usdm":
                result["usdm"] = settings
            used.add(kind)
        else:
            raise ValueError(f"unsupported leg {name}: {exchange}/{market}")
    return result


def validate_hyperliquid(config):
    if not config.get("enabled", False):
        return
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", config.get("address", "")):
        raise ValueError("hyperliquid.address must be the actual 0x account/subaccount address")
    rate = float(config.get("usdc_to_usdt", 1))
    if not math.isfinite(rate) or rate <= 0:
        raise ValueError("hyperliquid.usdc_to_usdt must be positive")

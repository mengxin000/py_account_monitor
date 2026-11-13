"""Validate OKX settings for the integrated service and standalone collector."""
import json
import math
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

try:
    from ..connections.okx.rest import OKXCredentials
except ImportError:
    from connections.okx.rest import OKXCredentials


@dataclass(frozen=True)
class OKXConfig:
    account_id: str
    output_dir: Path
    credentials: OKXCredentials = field(repr=False)
    inst_types: tuple[str, ...] = ("SPOT", "SWAP", "FUTURES")
    rest_base_url: str = "https://www.okx.com"
    websocket_url: str = "wss://ws.okx.com:8443/ws/v5/private"
    proxy: str | None = field(default=None, repr=False)
    simulated: bool = False
    rest_interval_seconds: float = 5
    reconcile_interval_seconds: float = 60


def load_okx_config(path):
    path = Path(path).resolve()
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    return build_okx_config(data, path)


def build_okx_config(data, path, *, isolated=True):
    path = Path(path).resolve()
    account_id = str(data.get("account_id", ""))
    if not re.fullmatch(r"[A-Za-z0-9_-]+", account_id):
        raise ValueError("OKX account_id must contain only letters, digits, '-' or '_'")
    settings = data.get("okx")
    if settings is None:
        candidates = [leg for leg in data.get("legs", {}).values() if leg.get("exchange") == "okx"]
        if len(candidates) != 1:
            raise ValueError("configure one okx object or one OKX leg")
        settings = candidates[0]
    if not isinstance(settings, dict) or settings.get("enabled", True) is not True:
        raise ValueError("OKX collection is disabled or configuration is invalid")
    settings = {**settings.get("credentials", {}), **settings}
    secrets = []
    for key in ("api_key", "secret_key", "passphrase"):
        value, env = settings.get(key), settings.get(key + "_env")
        if value and env:
            raise ValueError(f"configure {key} OR {key}_env")
        value = os.getenv(str(env)) if env else value
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"OKX {key} is missing")
        secrets.append(value)
    inst_types = settings.get("inst_types", ["SPOT", "SWAP", "FUTURES"])
    if not isinstance(inst_types, list) or not inst_types or any(t not in {"SPOT", "MARGIN", "SWAP", "FUTURES"} for t in inst_types):
        raise ValueError("invalid OKX inst_types")
    simulated = settings.get("simulated", False)
    if not isinstance(simulated, bool):
        raise ValueError("OKX simulated must be true or false")
    rest_interval = float(settings.get("rest_interval_seconds", data.get("rest_interval_seconds", 5)))
    reconcile_interval = float(settings.get("reconcile_interval_seconds", 60))
    if not math.isfinite(rest_interval) or rest_interval < 5:
        raise ValueError("OKX rest_interval_seconds must be at least 5")
    if not math.isfinite(reconcile_interval) or reconcile_interval < 30:
        raise ValueError("OKX reconcile_interval_seconds must be at least 30")
    project = path.parent.parent if path.parent.name == "config" else path.parent
    root = Path(data.get("output_dir", f"runtime/{account_id}"))
    if not root.is_absolute():
        root = project / root
    # Deliberate isolation: existing report/replay globbing must not consume OKX.
    root = root.resolve() / "okx" if isolated else root.resolve()
    default_ws = "wss://wspap.okx.com:8443/ws/v5/private" if simulated else "wss://ws.okx.com:8443/ws/v5/private"
    return OKXConfig(account_id, root, OKXCredentials(*secrets), tuple(dict.fromkeys(inst_types)),
                     str(settings.get("rest_base_url", "https://www.okx.com")),
                     str(settings.get("websocket_url", default_ws)),
                     settings.get("proxy", data.get("proxy")), simulated, rest_interval, reconcile_interval)

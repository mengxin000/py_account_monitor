"""Network diagnostics without URLs containing authentication secrets."""
from urllib.parse import urlsplit
import re


def connection_error(exc, endpoint):
    target = urlsplit(endpoint)
    parts = [type(exc).__name__, f"host={target.hostname}", f"port={target.port or 443}"]
    current = exc
    for _ in range(4):
        if current is None:
            break
        code = getattr(current, "errno", None)
        if code is not None:
            parts.append(f"errno={code}")
        status = getattr(current, "status", None)
        if status:
            parts.append(f"http_status={status}")
        match = re.search(r"['\"]code['\"]\s*:\s*(-\d+)", str(current))
        if match:
            parts.append(f"api_code={match.group(1)}")
        current = current.__cause__
    return " ".join(parts)

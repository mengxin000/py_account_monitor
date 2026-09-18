"""Source metadata is assigned by the connection, never guessed as Spot."""


def source_metadata(event, source):
    scope = "unknown"
    if source == "spot_stream":
        scope = "spot"
    elif source == "pm_stream":
        product = str(event.get("fs", "")).lower()
        if product in {"um", "cm"}:
            scope = product
        elif str(event.get("e", "")).upper() in {"EXECUTIONREPORT", "EXECUTION_REPORT"}:
            scope = "pm_margin"
    return {"exchange": "binance", "source": source, "accountScope": scope}

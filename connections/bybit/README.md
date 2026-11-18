# Bybit V5 connection scaffold

This adapter is deliberately read-only and raw-only in this phase:

- `rest.py`: signed V5 GET requests, currently no endpoint-specific account model.
- `user_stream.py`: private authentication, `order`, `execution`, `position`, and `wallet` topic subscriptions, reconnect, and backpressured callback delivery.
- `collectors/account_monitor.py`: stores received packets unchanged in the account trading-day directory as `bybit_callbacks.jsonl`.

Example account leg:

```json
{
  "legs": {
    "first": {
      "exchange": "bybit",
      "market": "linear",
      "credentials": {
        "api_key": "...",
        "secret_key": "..."
      }
    },
    "second": {
      "exchange": "binance",
      "market": "usdm",
      "credentials": {
        "api_key": "...",
        "secret_key": "..."
      }
    }
  }
}
```

Supported `market` selectors are currently `spot` and `linear`. This does not yet feed Bybit packets into the normalized fill pipeline, matching, equity, funding totals, or reports. No trading methods are implemented.

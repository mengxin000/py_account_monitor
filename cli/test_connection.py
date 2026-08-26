"""Read-only connectivity test for a Binance Portfolio Margin account.

Usage (PowerShell):
    Copy-Item credentials.local.example.json credentials.local.json
    # edit credentials.local.json, then:
    uv run python -m cli.test_connection

Environment variables BINANCE_API_KEY and BINANCE_SECRET_KEY take precedence
over the local JSON file.  This script only calls GET /papi/v1/account.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any

try:
    from ..core.connection import BinanceCredentials, BinanceRestClient
except ImportError:  # running from the project directory
    from core.connection import BinanceCredentials, BinanceRestClient


def load_credentials(path: Path) -> BinanceCredentials:
    values: dict[str, Any] = {}
    if path.exists():
        values = json.loads(path.read_text(encoding="utf-8"))

    api_key = os.getenv("BINANCE_API_KEY") or values.get("api_key") or values.get("apiKey")
    secret_key = os.getenv("BINANCE_SECRET_KEY") or values.get("secret_key") or values.get("secKey")
    if not api_key or not secret_key:
        raise SystemExit(
            "Missing credentials. Set BINANCE_API_KEY/BINANCE_SECRET_KEY or fill credentials.local.json."
        )
    return BinanceCredentials(
        api_key=api_key,
        secret_key=secret_key,
        subaccount_email=os.getenv("BINANCE_SUBACCOUNT_EMAIL")
        or values.get("subaccount_email")
        or values.get("email"),
        label=os.getenv("BINANCE_ACCOUNT_LABEL") or values.get("label"),
    )


def print_account_summary(payload: dict[str, Any], credentials: BinanceCredentials) -> None:
    print(f"account={credentials.label or '-'} subaccount={credentials.subaccount_email or '-'}")
    for key in (
        "uniMMR",
        "actualEquity",
        "accountEquity",
        "accountMaintMargin",
        "totalAvailableBalance",
        "totalAssetOfBtc",
        "totalLiabilityOfBtc",
    ):
        if key in payload:
            print(f"{key}: {payload[key]}")

    assets = payload.get("asset", payload.get("assets", []))
    # /papi/v1/account reports account-level risk/equity.  Asset rows are
    # returned separately by /papi/v1/balance and are passed in as `balance`.
    if not assets and isinstance(payload.get("balance"), list):
        assets = payload["balance"]
    if not isinstance(assets, list):
        print("raw response:", json.dumps(payload, ensure_ascii=False))
        return
    print("assets:")
    shown = 0
    for item in assets:
        if not isinstance(item, dict):
            continue
        amount = item.get("totalWalletBalance", item.get("walletBalance", item.get("free", "0")))
        try:
            non_zero = float(amount or 0) != 0
        except (TypeError, ValueError):
            non_zero = True
        if non_zero:
            print(
                f"  {item.get('asset', '?')}: "
                f"total={item.get('totalWalletBalance', '-')} "
                f"crossFree={item.get('crossMarginFree', '-')} "
                f"umWallet={item.get('umWalletBalance', '-')} "
                f"cmWallet={item.get('cmWalletBalance', '-')} "
                f"umPnL={item.get('umUnrealizedPNL', '-')} "
                f"cmPnL={item.get('cmUnrealizedPNL', '-')}"
            )
            shown += 1
    if shown == 0:
        print("  (no non-zero assets returned)")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only Binance Portfolio Margin account test")
    parser.add_argument(
        "--credentials",
        type=Path,
        default=Path(__file__).parents[1] / "config" / "credentials.local.json",
        help="local credentials JSON (ignored by git)",
    )
    args = parser.parse_args()
    credentials = load_credentials(args.credentials)
    async with BinanceRestClient(credentials) as client:
        # Portfolio Margin unified account endpoint; signed USER_DATA request.
        payload = await client.get("/papi/v1/account", signed=True)
        # Account information contains actualEquity, while this endpoint
        # contains per-asset balances.  Keep both reads separate in output.
        try:
            balance_response = await client.get("/papi/v1/balance", signed=True)
            balance_rows = balance_response.get("data", balance_response)
            if isinstance(balance_rows, list):
                payload["balance"] = balance_rows
        except Exception as exc:
            print(f"balance detail unavailable: {exc}")
    print_account_summary(payload, credentials)


if __name__ == "__main__":
    asyncio.run(main())

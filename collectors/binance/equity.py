"""Spot valuation in USDT; unknown assets are errors, never silently zero."""
from decimal import Decimal


def spot_equity(account, ticker_rows):
    if account.get("accountType") != "SPOT" or not isinstance(account.get("balances"), list):
        raise ValueError("ordinary Spot account balances unavailable")
    prices = {row["symbol"]: Decimal(str(row["price"])) for row in ticker_rows}
    def rate(asset):
        if asset == "USDT":
            return Decimal(1)
        direct = prices.get(asset + "USDT")
        if direct is not None and direct > 0:
            return direct
        for bridge in ("USDC", "BTC", "ETH"):
            leg = prices.get(asset + bridge)
            quote = prices.get(bridge + "USDT")
            if leg is not None and quote is not None and leg > 0 and quote > 0:
                return leg * quote
        raise ValueError(f"missing valuation price: {asset}")
    total = Decimal(0)
    assets = []
    for row in account["balances"]:
        quantity = Decimal(str(row["free"])) + Decimal(str(row["locked"]))
        if not quantity.is_finite():
            raise ValueError("non-finite Spot balance")
        if quantity == 0:
            continue
        price = rate(row["asset"])
        total += quantity * price
        assets.append({"asset": row["asset"], "quantity": str(quantity), "priceUSDT": str(price)})
    return float(total), assets

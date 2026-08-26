"""Small, dependency-light Binance connection layer used by the monitors.

This package deliberately contains transport and authentication only.  Order
book calculation, order matching and reporting belong to the monitor core.
"""

from .core.connection import (
    BinanceCredentials,
    BinanceRestClient,
    BinanceConnectionError,
    BinanceMarketStream,
    BinanceUserDataStream,
    UserStreamConfig,
)
from .core.legacy_matching import (
    ExposureMatch,
    LegacyMatcher,
    MatchOrder,
    MatchRecord,
    is_matching_order,
)

__all__ = [
    "BinanceCredentials",
    "BinanceRestClient",
    "BinanceConnectionError",
    "BinanceMarketStream",
    "BinanceUserDataStream",
    "UserStreamConfig",
    "ExposureMatch",
    "LegacyMatcher",
    "MatchOrder",
    "MatchRecord",
    "is_matching_order",
]

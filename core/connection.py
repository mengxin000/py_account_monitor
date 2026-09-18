"""Compatibility exports; transports live under connections/binance."""
try:
    from ..connections.binance.base import BinanceCredentials, BinanceConnectionError, UserStreamConfig
    from ..connections.binance.rest import BinanceRestClient
    from ..connections.binance.pm_stream import BinanceUserDataStream
    from ..connections.binance.market_stream import BinanceMarketStream
except ImportError:
    from connections.binance.base import BinanceCredentials, BinanceConnectionError, UserStreamConfig
    from connections.binance.rest import BinanceRestClient
    from connections.binance.pm_stream import BinanceUserDataStream
    from connections.binance.market_stream import BinanceMarketStream

"""
fetch_data.py
Provides fetch_ohlcv_threadsafe(symbol, timeframe, limit) -> pandas.DataFrame
This uses ccxt (blocking) but is safe to call inside asyncio via asyncio.to_thread(...)
"""

import ccxt
import pandas as pd
import logging

logger = logging.getLogger("fetch_data")

# create one exchange instance (no keys required for public data)
def _create_bybit():
    return ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "future"}})

_exchange = _create_bybit()

def _ohlcv_to_df(ohlcv):
    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
    return df

def fetch_ohlcv_threadsafe(symbol: str, timeframe: str = "15m", limit: int = 300):
    """
    Blocking call. Use inside asyncio.to_thread(...) when called from async code.
    """
    try:
        ohlcv = _exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        return _ohlcv_to_df(ohlcv)
    except Exception as e:
        logger.exception("fetch_ohlcv_threadsafe error for %s %s: %s", symbol, timeframe, e)
        raise

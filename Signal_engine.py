"""
signal_engine.py
Contains analyze_symbol(symbol, timeframe="15m") that returns:
{
  symbol, signal, entry, stop, tp, rsi, atr, vol, reason
}
"""

from typing import Dict
import math
import logging
import pandas as pd
import numpy as np
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange

from fetch_data import fetch_ohlcv_threadsafe

logger = logging.getLogger("signal_engine")

def compute_indicators(df: pd.DataFrame):
    df = df.copy()
    df["ema50"] = EMAIndicator(df["close"], window=50).ema_indicator()
    df["ema200"] = EMAIndicator(df["close"], window=200).ema_indicator()
    df["rsi14"] = RSIIndicator(df["close"], window=14).rsi()
    df["atr14"] = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=14).average_true_range()
    df["vol_sma20"] = df["volume"].rolling(window=20, min_periods=1).mean()
    return df

def detect_bullish_cross(df: pd.DataFrame):
    if len(df) < 2: 
        return False
    return (df["ema50"].iat[-1] > df["ema200"].iat[-1]) and (df["ema50"].iat[-2] <= df["ema200"].iat[-2])

def detect_bearish_cross(df: pd.DataFrame):
    if len(df) < 2:
        return False
    return (df["ema50"].iat[-1] < df["ema200"].iat[-1]) and (df["ema50"].iat[-2] >= df["ema200"].iat[-2])

def analyze_symbol(symbol: str, timeframe: str = "15m") -> Dict:
    try:
        df = fetch_ohlcv_threadsafe(symbol, timeframe=timeframe, limit=400)
        if df is None or df.shape[0] < 60:
            return {"symbol": symbol, "error": "not enough data"}
        df = compute_indicators(df)
        last = df.iloc[-1]
        # basic values
        entry = float(last["close"])
        rsi = float(last["rsi14"]) if not pd.isna(last["rsi14"]) else 50.0
        atr = float(last["atr14"]) if not pd.isna(last["atr14"]) else max(entry * 0.002, 0.0)
        vol_ok = last["volume"] > (last["vol_sma20"] * 1.2 if last["vol_sma20"] > 0 else 0)

        # 1m confirmation (best-effort)
        confirm_1m = True
        try:
            df1 = fetch_ohlcv_threadsafe(symbol, timeframe="1m", limit=120)
            df1 = compute_indicators(df1)
            confirm_1m = float(df1["close"].iat[-1]) > float(df1["ema50"].iat[-1])
        except Exception:
            confirm_1m = True

        reason = []
        signal = "NO SIGNAL"
        stop = None
        tp = None

        if detect_bullish_cross(df) and rsi < 40 and vol_ok and confirm_1m:
            signal = "LONG"
            stop = entry - atr * 1.5
            tp = entry + (entry - stop) * 2
            reason = [f"EMA50>EMA200", f"RSI {rsi:.1f} (<40)", "Vol OK"]
        elif detect_bearish_cross(df) and rsi > 60 and vol_ok and not confirm_1m:
            signal = "SHORT"
            stop = entry + atr * 1.5
            tp = entry - (stop - entry) * 2
            reason = [f"EMA50<EMA200", f"RSI {rsi:.1f} (>60)", "Vol OK"]
        else:
            reason = ["No clean EMA cross + confirmations"]

        return {
            "symbol": symbol,
            "signal": signal,
            "entry": round(entry, 8),
            "stop": round(stop, 8) if stop is not None else None,
            "tp": round(tp, 8) if tp is not None else None,
            "rsi": rsi,
            "atr": atr,
            "vol": float(last["volume"]),
            "reason": " | ".join(reason),
        }
    except Exception as e:
        logger.exception("analyze_symbol error")
        return {"symbol": symbol, "error": str(e)}

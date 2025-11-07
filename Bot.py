#!/usr/bin/env python3
"""
bot.py — main runner for CoinjaraNG Signals (15m timeframe).
- Periodically runs signals every 15 minutes and posts to SIGNAL_CHANNEL_ID.
- Also exposes /signal <SYMBOL> command for on-demand checks.
"""

import os
import logging
import asyncio
from dotenv import load_dotenv
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram import __version__ as tg_version

from fetch_data import fetch_ohlcv_threadsafe
from signal_engine import analyze_symbol

load_dotenv()

# ENV
BOT_TOKEN = os.getenv("BOT_TOKEN")
SIGNAL_CHANNEL_ID = os.getenv("SIGNAL_CHANNEL_ID")  # e.g. @channelname or numeric chat id
MODE = os.getenv("MODE", "PAPER").upper()  # PAPER or LIVE
SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,ADAUSDT").split(",")

if not BOT_TOKEN or not SIGNAL_CHANNEL_ID:
    raise SystemExit("Set BOT_TOKEN and SIGNAL_CHANNEL_ID in .env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("coinjarang")

# helper to format and send text
async def send_text(app, chat_id, text):
    try:
        await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
    except Exception as e:
        logger.exception("send_text failed: %s", e)

def format_signal(res):
    if res.get("error"):
        return f"*{res.get('symbol')}* — Error: {res.get('error')}"
    if res.get("signal") == "NO SIGNAL":
        return f"*{res['symbol']}* — _No clear signal_\nRSI: {res['rsi']:.1f}\nReason: {res['reason']}"
    return (
        f"*{res['symbol']}* — *{res['signal']}*\n"
        f"Entry: `{res['entry']}`\nStop: `{res['stop']}`\nTP: `{res['tp']}`\n"
        f"RSI: {res['rsi']:.1f}\nMode: {MODE}\nReason: {res['reason']}"
    )

async def run_signals(app):
    logger.info("Running scheduled signals for symbols: %s", SYMBOLS)
    # run sequentially to avoid rate limits
    for symbol in SYMBOLS:
        symbol = symbol.strip().upper()
        try:
            # fetch & analyze in thread-safe manner (ccxt is blocking)
            res = await asyncio.to_thread(analyze_symbol, symbol, timeframe="15m")
            text = format_signal(res)
            await send_text(app, SIGNAL_CHANNEL_ID, text)
            await asyncio.sleep(0.6)  # small delay
        except Exception:
            logger.exception("Failed running signal for %s", symbol)

# Telegram command handlers
async def start_cmd(update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "CoinjaraNG Signals (PAPER mode)\n\nCommands:\n"
        "/signal <SYMBOL> — get single symbol analysis\n"
        "Bot auto-posts premium signals to configured channel (admin controlled)."
    )

async def signal_cmd(update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /signal BTCUSDT")
        return
    symbol = args[0].upper()
    msg = await update.message.reply_text(f"Analyzing {symbol} ...")
    try:
        res = await asyncio.to_thread(analyze_symbol, symbol, timeframe="15m")
        text = format_signal(res)
        await msg.edit_text(text, parse_mode="Markdown")
    except Exception as e:
        logger.exception("signal_cmd error")
        await msg.edit_text(f"Error analyzing {symbol}: {e}")

async def run_signals_cmd(update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Running signals now (manual trigger)...")
    await run_signals(context.application)

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("signal", signal_cmd))
    app.add_handler(CommandHandler("run_signals", run_signals_cmd))  # admin trigger

    # schedule background job using JobQueue (runs every 15 minutes)
    job_queue = app.job_queue
    # first run in ~10s, then every 15 minutes (900 sec)
    job_queue.run_repeating(lambda ctx: asyncio.create_task(run_signals(ctx.application)), interval=900, first=10)

    logger.info("Starting Telegram bot (python-telegram-bot %s) ...", tg_version)
    app.run_polling()

if __name__ == "__main__":
    main()

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys

# ---------------------------------------------
# Ensure output directory
# ---------------------------------------------
os.makedirs("output", exist_ok=True)

# ---------------------------------------------
# Detect run mode (MORNING or EOD)
# ---------------------------------------------
RUN_MODE = os.getenv("RUN_MODE", "MORNING")  # default morning
print(f"⚙️ RUN MODE: {RUN_MODE}")

# ---------------------------------------------
# Load stock list
# ---------------------------------------------
stocks = pd.read_csv("stocks.csv")["Symbol"].tolist()
stocks = [s.strip().upper() for s in stocks]

# ---------------------------------------------
# Data fetcher
# ---------------------------------------------
def fetch_data(symbol):
    try:
        df = yf.download(symbol, period="5y", interval="1d",
                         auto_adjust=True, progress=False)
        if df is not None and not df.empty:
            return df
    except:
        pass
    return pd.DataFrame()


# ---------------------------------------------
# Technical indicators
# ---------------------------------------------
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def rsi(series, period=14):
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    gain_ema = pd.Series(gain).ewm(span=period).mean()
    loss_ema = pd.Series(loss).ewm(span=period).mean()
    rs = gain_ema / loss_ema
    return 100 - (100 / (1 + rs))


# ---------------------------------------------
# Output containers
# ---------------------------------------------
df_fund = []
df_tech = []
df_vol = []

# ---------------------------------------------
# PROCESS STOCKS
# ---------------------------------------------
for symbol in stocks:
    print(f"Processing: {symbol}")

    data = fetch_data(symbol)
    if data is None or data.empty:
        print(f"⚠ No data for {symbol}, skipping.")
        continue

    close = data["Close"].squeeze()
    high  = data["High"].squeeze()
    low   = data["Low"].squeeze()

    latest = close.iloc[-1]

    # ---- Price history ----
    price_1D = close.iloc[-2] if len(close) >= 2 else np.nan
    price_1W = close.iloc[-5] if len(close) >= 5 else np.nan
    price_1M = close.iloc[-22] if len(close) >= 22 else np.nan
    price_3M = close.iloc[-66] if len(close) >= 66 else np.nan
    price_6M = close.iloc[-132] if len(close) >= 132 else np.nan
    price_1Y = close.iloc[-252] if len(close) >= 252 else np.nan

    # ---- Returns ----
    def pct(a, b):
        return ((a - b) / b * 100) if b and b != 0 and not pd.isna(b) else np.nan

    r1d = pct(latest, price_1D)
    r1w = pct(latest, price_1W)
    r1m = pct(latest, price_1M)
    r3m = pct(latest, price_3M)
    r6m = pct(latest, price_6M)
    r1y = pct(latest, price_1Y)

    # Always append fundamentals
    df_fund.append([
        symbol, latest,
        price_1D, price_1W, price_1M, price_3M, price_6M, price_1Y,
        r1d, r1w, r1m, r3m, r6m, r1y
    ])

    # EOD mode: skip technicals & volatility
    if RUN_MODE == "EOD":
        continue

    # ---- RSI ----
    rsi_val = rsi(close).iloc[-1]

    # ---- EMAs ----
    ema20 = ema(close, 20).iloc[-1]
    ema50 = ema(close, 50).iloc[-1]
    ema100 = ema(close, 100).iloc[-1]

    # ---- MACD ----
    ema12 = ema(close, 12)
    ema26 = ema(close, 26)
    macd_line = ema12 - ema26
    signal = ema(macd_line, 9)
    histogram = macd_line - signal

    df_tech.append([
        symbol, rsi_val,
        macd_line.iloc[-1], macd_line.iloc[-2],
        signal.iloc[-1], signal.iloc[-2],
        histogram.iloc[-1] - histogram.iloc[-2],
        ema20, ema50, ema100,
        price_1W, price_1M, price_3M
    ])

    # ---- Volatility ----
    weekly = close.resample("W").last()
    weekly_stddev = weekly.pct_change().std() * 100
    weekly_atr = (high - low).resample("W").mean().iloc[-1]
    wow = pct(weekly.iloc[-1], weekly.iloc[-2]) if len(weekly) >= 2 else np.nan
    trend_13 = pct(weekly.iloc[-1], weekly.iloc[-13]) if len(weekly) >= 13 else np.nan

    df_vol.append([
        symbol, weekly_stddev, weekly_atr, wow, trend_13
    ])


# ---------------------------------------------
# SAVE OUTPUTS
# ---------------------------------------------
timestamp = "0830" if RUN_MODE == "MORNING" else "1530"

# Fundamentals always saved
pd.DataFrame(df_fund, columns=[
    "Symbol","Latest_Close","Price_1D","Price_1W","Price_1M","Price_3M","Price_6M","Price_1Y",
    "Return_1D","Return_1W","Return_1M","Return_3M","Return_6M","Return_1Y"
]).to_csv(f"output/fundamentals_{timestamp}.csv", index=False)

# Technicals & Vol only in morning mode
if RUN_MODE == "MORNING":
    pd.DataFrame(df_tech, columns=[
        "Symbol","RSI","MACD_Latest","MACD_1D","Signal_Latest","Signal_1D",
        "Histogram_Trend","EMA20","EMA50","EMA100",
        "Price_1W","Price_1M","Price_3M"
    ]).to_csv("output/technicals_0830.csv", index=False)

    pd.DataFrame(df_vol, columns=[
        "Symbol","Weekly_StdDev","Weekly_ATR","Weekly_WoW","Trend_13W"
    ]).to_csv("output/volatility_0830.csv", index=False)

print("\n🎉 RUN COMPLETE — Mode:", RUN_MODE)

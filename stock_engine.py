import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import traceback

# -------------------------------------------------------
# Ensure output directory exists
# -------------------------------------------------------
os.makedirs("output", exist_ok=True)

# -------------------------------------------------------
# Load & clean stock symbols
# -------------------------------------------------------
stocks = pd.read_csv("stocks.csv")["Symbol"].tolist()
stocks = [s.strip().upper() for s in stocks]
print("Total stocks loaded:", len(stocks))

# -------------------------------------------------------
# INDMoney-style trading day offsets
# These will be validated after first run; adjust by ±1 if needed
# -------------------------------------------------------
OFFSETS = {
    "1D": 1,
    "1W": 5,
    "1M": 22,
    "3M": 66,
    "6M": 132,
    "1Y": 264
}

# -------------------------------------------------------
# Robust data fetcher
# -------------------------------------------------------
def fetch_data(symbol):
    """Fetch ~400 trading days of daily data with multiple fallbacks."""
    try:
        df = yf.download(symbol, period="400d", interval="1d",
                         auto_adjust=True, progress=False)
        if df is not None and not df.empty:
            return df
    except:
        pass

    try:
        df = yf.Ticker(symbol).history(period="400d", auto_adjust=True)
        if df is not None and not df.empty:
            return df
    except:
        pass

    return pd.DataFrame()


# -------------------------------------------------------
# Helper functions
# -------------------------------------------------------
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


# -------------------------------------------------------
# Output containers
# -------------------------------------------------------
df_fund = []
df_tech = []
df_vol = []

# -------------------------------------------------------
# PROCESS EACH STOCK
# -------------------------------------------------------
for symbol in stocks:
    try:
        print(f"\n🔄 Processing: {symbol}")

        data = fetch_data(symbol)
        if data.empty:
            print(f"⚠ WARNING: No data for {symbol} — skipping")
            continue

        close = data["Close"].squeeze()
        high  = data["High"].squeeze()
        low   = data["Low"].squeeze()

        latest = close.iloc[-1]

        # -------------- Fundamentals (INDMoney-style) --------------
        prices = {}
        returns = {}

        for label, offset in OFFSETS.items():
            if len(close) > offset:
                past_price = close.iloc[-(offset + 1)]
                prices[label] = past_price
                returns[label] = (latest - past_price) / past_price * 100
            else:
                prices[label] = np.nan
                returns[label] = np.nan

        # -------------- Technical Indicators ------------------------
        rsi_val = rsi(close).iloc[-1]

        ema20 = ema(close, 20).iloc[-1]
        ema50 = ema(close, 50).iloc[-1]
        ema100 = ema(close, 100).iloc[-1]

        ema12 = ema(close, 12)
        ema26 = ema(close, 26)
        macd_line = ema12 - ema26
        signal = ema(macd_line, 9)
        histogram = macd_line - signal

        macd_latest = macd_line.iloc[-1]
        macd_1d = macd_line.iloc[-2]
        signal_latest = signal.iloc[-1]
        signal_1d = signal.iloc[-2]
        histogram_trend = histogram.iloc[-1] - histogram.iloc[-2]

        # -------------- Volatility ---------------------------------
        weekly = close.resample("W").last()

        weekly_stddev = weekly.pct_change().std() * 100
        weekly_atr = (high - low).resample("W").mean().iloc[-1]
        weekly_wow = ((weekly.iloc[-1] - weekly.iloc[-2]) /
                      weekly.iloc[-2] * 100) if len(weekly) >= 2 else np.nan
        trend_13w = ((weekly.iloc[-1] - weekly.iloc[-13]) /
                     weekly.iloc[-13] * 100) if len(weekly) >= 13 else np.nan

        # -------------- Append to Fundamentals ----------------------
        df_fund.append([
            symbol,
            latest,
            prices["1W"], prices["1M"], prices["3M"],
            prices["6M"], prices["1Y"],
            returns["1D"], returns["1W"], returns["1M"],
            returns["3M"], returns["6M"], returns["1Y"]
        ])

        # -------------- Append to Technicals ------------------------
        df_tech.append([
            symbol,
            rsi_val, macd_latest, macd_1d,
            signal_latest, signal_1d, histogram_trend,
            ema20, ema50, ema100,
            prices["1W"], prices["1M"], prices["3M"]
        ])

        # -------------- Append to Volatility ------------------------
        df_vol.append([
            symbol,
            weekly_stddev, weekly_atr, weekly_wow, trend_13w
        ])

    except Exception as e:
        print(f"❌ ERROR in {symbol}: {e}")
        traceback.print_exc()
        continue


# -------------------------------------------------------
# SAVE ALL OUTPUTS
# -------------------------------------------------------
pd.DataFrame(df_fund, columns=[
    "Symbol","Latest_Close",
    "Price_1W","Price_1M","Price_3M","Price_6M","Price_1Y",
    "Return_1D","Return_1W","Return_1M",
    "Return_3M","Return_6M","Return_1Y"
]).to_csv("output/fundamentals.csv", index=False)

pd.DataFrame(df_tech, columns=[
    "Symbol","RSI","MACD_Latest","MACD_1D","Signal_Latest",
    "Signal_1D","Histogram_Trend","EMA20","EMA50","EMA100",
    "Price_1W","Price_1M","Price_3M"
]).to_csv("output/technicals.csv", index=False)

pd.DataFrame(df_vol, columns=[
    "Symbol","Weekly_StdDev","Weekly_ATR","Weekly_WoW","Trend_13W"
]).to_csv("output/volatility.csv", index=False)

print("\n🎉 SUCCESS — All 3 CSV files generated with INDMoney-style fundamentals!\n")

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# -------------------------------------------------------
# Ensure output folder exists
# -------------------------------------------------------
os.makedirs("output", exist_ok=True)

# -------------------------------------------------------
# Load & clean symbols
# -------------------------------------------------------
stocks = pd.read_csv("stocks.csv")["Symbol"].tolist()
stocks = [s.strip().upper() for s in stocks]
print("Total stocks loaded:", len(stocks))

# -------------------------------------------------------
# Robust data fetcher (fixes NSE empty data issues)
# -------------------------------------------------------
def fetch_data(symbol):
    # Method 1 — normal download
    try:
        df = yf.download(symbol, period="5y", interval="1d",
                         auto_adjust=True, progress=False)
        if df is not None and not df.empty:
            return df
    except:
        pass

    # Method 2 — ticker history
    try:
        df = yf.Ticker(symbol).history(period="5y", auto_adjust=True)
        if df is not None and not df.empty:
            return df
    except:
        pass

    # Method 3 — manual date range fetch
    try:
        df = yf.download(
            symbol,
            start=(datetime.now() - timedelta(days=1825)).strftime("%Y-%m-%d"),
            end=datetime.now().strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=True,
            progress=False
        )
        if df is not None and not df.empty:
            return df
    except:
        pass

    return pd.DataFrame()  # complete failure fallback

# -------------------------------------------------------
# Helper functions (stable versions)
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
# Output tables
# -------------------------------------------------------
df_fund = []
df_tech = []
df_vol = []

# -------------------------------------------------------
# PROCESS EACH STOCK
# -------------------------------------------------------
for symbol in stocks:
    try:
        print(f"Processing: {symbol}")

        data = fetch_data(symbol)

        if data is None or data.empty:
            print(f"⚠ WARNING: No data for {symbol} — skipping")
            continue

        # FIX: Yahoo returns Close sometimes as 2-D. Convert to 1-D.
        close = data["Close"].squeeze()

        # ---------- Returns ----------
        latest = close.iloc[-1]
        return_1D = (close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100
        return_1W = (close.iloc[-1] - close.iloc[-5]) / close.iloc[-5] * 100 if len(close) >= 5 else np.nan
        return_1M = (close.iloc[-1] - close.iloc[-22]) / close.iloc[-22] * 100 if len(close) >= 22 else np.nan
        return_3M = (close.iloc[-1] - close.iloc[-66]) / close.iloc[-66] * 100 if len(close) >= 66 else np.nan
        return_6M = (close.iloc[-1] - close.iloc[-132]) / close.iloc[-132] * 100 if len(close) >= 132 else np.nan
        return_1Y = (close.iloc[-1] - close.iloc[-252]) / close.iloc[-252] * 100 if len(close) >= 252 else np.nan

        # ---------- RSI ----------
        rsi_val = rsi(close).iloc[-1]

        # ---------- EMA ----------
        ema20 = ema(close, 20).iloc[-1]
        ema50 = ema(close, 50).iloc[-1]
        ema100 = ema(close, 100).iloc[-1]

        # ---------- MACD ----------
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

        # ---------- Price Levels ----------
        price_20 = close.iloc[-20] if len(close) >= 20 else np.nan
        price_50 = close.iloc[-50] if len(close) >= 50 else np.nan
        price_100 = close.iloc[-100] if len(close) >= 100 else np.nan

        # ---------- Volatility ----------
        weekly = close.resample("W").last()

        weekly_stddev = weekly.pct_change().std() * 100
        weekly_atr = (data["High"] - data["Low"]).resample("W").mean().iloc[-1]
        weekly_wow = ((weekly.iloc[-1] - weekly.iloc[-2]) / weekly.iloc[-2] * 100) if len(weekly) >= 2 else np.nan
        trend_13w = ((weekly.iloc[-1] - weekly.iloc[-13]) / weekly.iloc[-13] * 100) if len(weekly) >= 13 else np.nan

        # ---------- Append ----------
        df_fund.append([
            symbol, latest, return_1D, return_1W, return_1M,
            return_3M, return_6M, return_1Y
        ])

        df_tech.append([
            symbol, rsi_val, macd_latest, macd_1d, signal_latest, signal_1d,
            histogram_trend, ema20, ema50, ema100,
            price_20, price_50, price_100
        ])

        df_vol.append([
            symbol, weekly_stddev, weekly_atr, weekly_wow, trend_13w
        ])

    except Exception as e:
        print(f"❌ Error for {symbol}: {e}")
        continue


# -------------------------------------------------------
# SAVE OUTPUT FILES
# -------------------------------------------------------
pd.DataFrame(df_fund, columns=[
    "Symbol","Latest_Close","Return_1D","Return_1W",
    "Return_1M","Return_3M","Return_6M","Return_1Y"
]).to_csv("output/fundamentals.csv", index=False)

pd.DataFrame(df_tech, columns=[
    "Symbol","RSI","MACD_Latest","MACD_1D","Signal_Latest",
    "Signal_1D","Histogram_Trend","EMA20","EMA50","EMA100",
    "Price_20","Price_50","Price_100"
]).to_csv("output/technicals.csv", index=False)

pd.DataFrame(df_vol, columns=[
    "Symbol","Weekly_StdDev","Weekly_ATR","Weekly_WoW","Trend_13W"
]).to_csv("output/volatility.csv", index=False)

print("\n🎉 SUCCESS! All CSV files generated with data.\n")

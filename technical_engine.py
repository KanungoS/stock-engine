import os
import requests
import pandas as pd
import numpy as np
import talib
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
API_KEY = "acedb5f1fa8b4604a82e9207adb9e30c"
BASE_URL = "https://api.twelvedata.com/time_series"

STOCK_LIST = "stocks.csv"  # file containing symbols (1 per line)
OUTPUT_MASTER = "Master_Technical_Sheet.xlsx"

# -----------------------------
# FETCH OHLCV From TWELVEDATA
# -----------------------------
def fetch_stock_data(symbol, interval="1day", outputsize=5000):
    params = {
        "symbol": symbol,
        "interval": interval,
        "apikey": API_KEY,
        "outputsize": outputsize,
        "format": "JSON"
    }

    try:
        r = requests.get(BASE_URL, params=params, timeout=20)
        data = r.json()

        if "values" not in data:
            return None

        df = pd.DataFrame(data["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime")

        # convert to numeric
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna()
        return df

    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

# -----------------------------
# TECHNICAL ANALYSIS ENGINE
# -----------------------------
def compute_indicators(df):
    close = df["close"].values
    high = df["high"].values
    low = df["low"].values
    volume = df["volume"].values

    df["SMA_50"] = talib.SMA(close, timeperiod=50)
    df["SMA_100"] = talib.SMA(close, timeperiod=100)
    df["SMA_200"] = talib.SMA(close, timeperiod=200)

    df["RSI_14"] = talib.RSI(close, timeperiod=14)
    macd, macdsignal, macdhist = talib.MACD(close, 12, 26, 9)
    df["MACD"] = macd
    df["MACD_Hist"] = macdhist
    df["ADX_14"] = talib.ADX(high, low, close, timeperiod=14)

    upper, middle, lower = talib.BBANDS(close, timeperiod=20)
    df["BB_Upper"] = upper
    df["BB_Lower"] = lower

    return df

# -----------------------------
# TECHNICAL SCORE ENGINE
# -----------------------------
def compute_score(df):
    latest = df.iloc[-1]

    score = 0

    # Trend scoring
    if latest["close"] > latest["SMA_50"]:
        score += 10
    if latest["close"] > latest["SMA_100"]:
        score += 10
    if latest["close"] > latest["SMA_200"]:
        score += 15

    # RSI scoring
    if latest["RSI_14"] < 30:
        score += 5
    elif latest["RSI_14"] < 40:
        score += 3
    elif latest["RSI_14"] > 70:
        score -= 5

    # MACD Trend
    if latest["MACD_Hist"] > 0:
        score += 8

    # ADX strength
    if latest["ADX_14"] > 25:
        score += 5
    if latest["ADX_14"] > 30:
        score += 5

    # Bollinger breakout
    if latest["close"] > latest["BB_Upper"]:
        score -= 5  # overbought breakout

    if latest["close"] < latest["BB_Lower"]:
        score += 5  # oversold reversal

    return score

# -----------------------------
# PROCESS STOCK LIST
# -----------------------------
def run_engine():
    symbols = pd.read_csv(STOCK_LIST)["Symbol"].tolist()
    final_rows = []

    for sym in symbols:
        print(f"Processing {sym}...")

        df = fetch_stock_data(sym)
        if df is None or len(df) < 200:
            print(f"Skipping {sym}, insufficient data.")
            continue

        df = compute_indicators(df)
        score = compute_score(df)

        latest = df.iloc[-1]

        final_rows.append({
            "Symbol": sym,
            "Date": latest["datetime"],
            "Close": latest["close"],
            "SMA_50": latest["SMA_50"],
            "SMA_100": latest["SMA_100"],
            "SMA_200": latest["SMA_200"],
            "RSI_14": latest["RSI_14"],
            "MACD": latest["MACD"],
            "MACD_Hist": latest["MACD_Hist"],
            "ADX_14": latest["ADX_14"],
            "Score": score
        })

    master_df = pd.DataFrame(final_rows).sort_values("Score", ascending=False)

    # Create Top Lists
    top20_short = master_df.nlargest(20, "Score")
    mid_df = master_df[(master_df.Score > 10) & (master_df.Score <= 25)].nlargest(20, "Score")
    long_df = master_df[(master_df.Score <= 10)].nlargest(20, "Score")
    star5 = master_df.nlargest(5, "Score")

    # Save Excel
    with pd.ExcelWriter(OUTPUT_MASTER) as writer:
        master_df.to_excel(writer, sheet_name="Master", index=False)
        top20_short.to_excel(writer, sheet_name="Top20_ShortTerm", index=False)
        mid_df.to_excel(writer, sheet_name="Top20_MediumTerm", index=False)
        long_df.to_excel(writer, sheet_name="Top20_LongTerm", index=False)
        star5.to_excel(writer, sheet_name="Star_Top5", index=False)

    print("\n✔ Technical Engine Completed.")
    print(f"✔ Excel generated: {OUTPUT_MASTER}")

# -----------------------------
# RUN ENGINE
# -----------------------------
if __name__ == "__main__":
    run_engine()

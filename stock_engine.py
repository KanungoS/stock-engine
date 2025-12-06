import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ------------------------------------------------------------
# Utility Functions
# ------------------------------------------------------------

def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def rsi(series, period=14):
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    gain_ema = pd.Series(gain).ewm(span=period, adjust=False).mean()
    loss_ema = pd.Series(loss).ewm(span=period, adjust=False).mean()
    rs = gain_ema / loss_ema
    return 100 - (100 / (1 + rs))

def safe(v):
    try:
        return float(v)
    except:
        return np.nan

# ------------------------------------------------------------
# Get Last 13 Fridays
# ------------------------------------------------------------
def get_last_13_fridays():
    today = datetime.now().date()
    last_friday = today - timedelta(days=(today.weekday() - 4) % 7)
    fridays = [last_friday - timedelta(weeks=i) for i in range(13)]
    return list(reversed(fridays))  # oldest → newest

# ------------------------------------------------------------
# Load Stock List
# ------------------------------------------------------------
stocks = pd.read_csv("stocks.csv")["Symbol"].tolist()

# ------------------------------------------------------------
# Output Row Lists
# ------------------------------------------------------------
fundamental_rows = []
technical_rows = []
volatility_rows = []
combined_rows = []

fridays = get_last_13_fridays()

# ------------------------------------------------------------
# MAIN PROCESSING LOOP
# ------------------------------------------------------------
for symbol in stocks:
    print("Processing:", symbol)

    data = yf.download(symbol, period="400d", interval="1d", progress=False)

    if data.empty:
        continue

    data["Close"] = data["Close"].astype(float)

    # ----------------------
    # FUNDAMENTAL PRICES
    # ----------------------
    price_latest = safe(data["Close"].iloc[-1])

    def get_price(days_ago):
        try:
            return safe(data["Close"].iloc[-days_ago - 1])
        except:
            return np.nan

    price_1d = get_price(1)
    price_5d = get_price(5)
    price_1m = get_price(20)
    price_3m = get_price(60)
    price_6m = get_price(120)
    price_1y = get_price(252)

    def ret(past):
        if past and price_latest:
            return (price_latest - past) / past * 100
        return np.nan

    ret_1d = ret(price_1d)
    ret_5d = ret(price_5d)
    ret_1m = ret(price_1m)
    ret_3m = ret(price_3m)
    ret_6m = ret(price_6m)
    ret_1y = ret(price_1y)

    # ----------------------
    # TECHNICALS
    # ----------------------
    closes = data["Close"]
    rsi_val = safe(rsi(closes).iloc[-1])

    ema12 = ema(closes, 12)
    ema26 = ema(closes, 26)
    macd = ema12 - ema26
    signal = ema(macd, 9)

    macd_latest = safe(macd.iloc[-1])
    macd_1d = safe(macd.iloc[-2])
    signal_latest = safe(signal.iloc[-1])
    signal_1d = safe(signal.iloc[-2])

    histogram_trend = (macd_latest - signal_latest) - (macd_1d - signal_1d)

    ema20 = safe(ema(closes, 20).iloc[-1])
    ema50 = safe(ema(closes, 50).iloc[-1])
    ema100 = safe(ema(closes, 100).iloc[-1])

    price_20 = price_latest - ema20
    price_50 = price_latest - ema50
    price_100 = price_latest - ema100

    # ----------------------
    # VOLATILITY (13 weeks)
    # ----------------------
    weekly_prices = []

    for f in fridays:
        row = data[data.index.date == f]
        if not row.empty:
            weekly_prices.append(safe(row["Close"].iloc[-1]))
        else:
            # fallback: nearest
            closest = data.iloc[(data.index.date - f).abs().argmin()]
            weekly_prices.append(safe(closest["Close"]))

    weekly_series = pd.Series(weekly_prices)

    weekly_wow = (weekly_series.iloc[-1] - weekly_series.iloc[-2]) / weekly_series.iloc[-2] * 100
    weekly_stddev = weekly_series.pct_change().std() * 100
    weekly_atr = (weekly_series.diff().abs().mean() / weekly_series.mean()) * 100
    trend_13w = weekly_series.diff().sum() / weekly_series.mean()

    # ----------------------
    # APPEND ROWS
    # ----------------------
    fundamental_rows.append([
        symbol, price_latest, price_1d, price_5d, price_1m, price_3m,
        price_6m, price_1y, ret_1d, ret_5d, ret_1m, ret_3m, ret_6m, ret_1y
    ])

    technical_rows.append([
        symbol, rsi_val, macd_latest, macd_1d, signal_latest, signal_1d,
        histogram_trend, ema20, ema50, ema100, price_20, price_50, price_100
    ])

    volatility_rows.append([
        symbol, weekly_stddev, weekly_atr, weekly_wow, trend_13w
    ])

    # 🔥 NO BRACES, NO DICTS → CLEAN LIST
    combined_rows.append([
        symbol, price_latest, ret_1d, ret_1m, rsi_val,
        macd_latest, signal_latest, ema20, price_20,
        weekly_wow, weekly_atr, trend_13w
    ])

# ------------------------------------------------------------
# EXPORT 4 CLEAN CSV FILES
# ------------------------------------------------------------
pd.DataFrame(fundamental_rows,
    columns=[
        "Symbol","Price_Latest","Price_1D_Ago","Price_5D_Ago","Price_1M_Ago",
        "Price_3M_Ago","Price_6M_Ago","Price_1Y_Ago",
        "Return_1D","Return_5D","Return_1M","Return_3M","Return_6M","Return_1Y"
    ]).to_csv("fundamental_scores.csv", index=False)

pd.DataFrame(technical_rows,
    columns=[
        "Symbol","RSI_Latest","MACD_Latest","MACD_1D","Signal_Latest","Signal_1D",
        "Histogram_Trend","EMA20","EMA50","EMA100",
        "Price_20","Price_50","Price_100"
    ]).to_csv("technical_scores.csv", index=False)

pd.DataFrame(volatility_rows,
    columns=[
        "Symbol","Weekly_StdDev_Pct","Weekly_ATR_Pct",
        "Weekly_WoW_Pct","Weekly_Trend_13W"
    ]).to_csv("weekly_volatility.csv", index=False)

pd.DataFrame(combined_rows,
    columns=[
        "Symbol","Price_Latest","Return_1D","Return_1M","RSI_Latest",
        "MACD_Latest","Signal_Latest","EMA20","Price_20",
        "Weekly_WoW_Pct","Weekly_ATR_Pct","Weekly_Trend_13W"
    ]).to_csv("master_scores.csv", index=False)

print("All 4 files created successfully!")


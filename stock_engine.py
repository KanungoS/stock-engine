import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import pytz
import os
import shutil

# --------------------------------------------------------------
# Helper: Ensure folders exist
# --------------------------------------------------------------
BASE = "output"
ARCHIVE = f"{BASE}/archive"
os.makedirs(BASE, exist_ok=True)
os.makedirs(ARCHIVE, exist_ok=True)

# --------------------------------------------------------------
# Load symbols
# --------------------------------------------------------------
stocks = pd.read_csv("stocks.csv")["Symbol"].tolist()
stocks = [s.strip().upper() for s in stocks]

# --------------------------------------------------------------
# Determine Run Mode
# --------------------------------------------------------------
run_mode = os.getenv("RUN_MODE", "AUTO").upper()

print(f"🔧 RUN_MODE (raw) = {run_mode}")

# If manual run did not send mode → treat as 0830
if run_mode == "AUTO":
    run_mode = "0830"

# --------------------------------------------------------------
# IST Time
# --------------------------------------------------------------
utc_now = datetime.now(timezone.utc)
ist = pytz.timezone("Asia/Kolkata")
now_ist = utc_now.astimezone(ist)
date_str = now_ist.strftime("%Y-%m-%d")

print(f"🕒 IST Now = {now_ist}")

# --------------------------------------------------------------
# File paths
# --------------------------------------------------------------
fund_0830 = f"{BASE}/fundamentals_0830.csv"
fund_1530 = f"{BASE}/fundamentals_1530.csv"

tech_0830 = f"{BASE}/technicals_0830.csv"
vol_0830 = f"{BASE}/volatility_0830.csv"

# --------------------------------------------------------------
# Fetcher
# --------------------------------------------------------------
def fetch(symbol):
    try:
        df = yf.download(symbol, period="5y", interval="1d", auto_adjust=True, progress=False)
        if df is not None and not df.empty:
            return df
    except:
        pass
    return pd.DataFrame()

# --------------------------------------------------------------
# Fundamentals function
# --------------------------------------------------------------
def compute_fundamentals(close_series):
    latest = close_series.iloc[-1]
    def lag(days):
        return close_series.iloc[-days] if len(close_series) > days else np.nan
    def ret(days):
        return ((latest - lag(days)) / lag(days) * 100
                if not pd.isna(lag(days)) else np.nan)
    return latest, [
        lag(1), lag(5), lag(22), lag(66), lag(132), lag(252),
        ret(1), ret(5), ret(22), ret(66), ret(132), ret(252)
    ]

# --------------------------------------------------------------
# Technicals function
# --------------------------------------------------------------
def compute_technicals(close, high, low):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    rs = gain.ewm(14).mean() / loss.ewm(14).mean()
    rsi = 100 - 100/(1 + rs.iloc[-1])

    ema = lambda s, p: s.ewm(span=p).mean()
    ema20 = ema(close, 20).iloc[-1]
    ema50 = ema(close, 50).iloc[-1]
    ema100 = ema(close, 100).iloc[-1]

    macd_line = ema(close, 12) - ema(close, 26)
    signal = ema(macd_line, 9)
    hist_trend = (macd_line - signal).iloc[-1] - (macd_line - signal).iloc[-2]

    return rsi, ema20, ema50, ema100, macd_line, signal, hist_trend

# --------------------------------------------------------------
# Volatility function
# --------------------------------------------------------------
def compute_vol(close, high, low):
    weekly = close.resample("W").last()
    std = weekly.pct_change().std() * 100
    atr = (high - low).resample("W").mean().iloc[-1]
    wow = ((weekly.iloc[-1] - weekly.iloc[-2]) /
           weekly.iloc[-2] * 100) if len(weekly) > 2 else np.nan
    trend13 = ((weekly.iloc[-1] - weekly.iloc[-13]) /
               weekly.iloc[-13] * 100) if len(weekly) > 13 else np.nan
    return std, atr, wow, trend13

# --------------------------------------------------------------
# MAIN RUN LOGIC
# --------------------------------------------------------------
def run_snapshot(label):
    print(f"📌 Generating {label} snapshot...")

    fund_rows = []
    tech_rows = []
    vol_rows = []

    for sym in stocks:
        data = fetch(sym)
        if data.empty:
            continue

        close = data["Close"].squeeze()
        high = data["High"].squeeze()
        low = data["Low"].squeeze()

        latest, fvals = compute_fundamentals(close)
        fund_rows.append([sym, latest] + fvals)

        # Only 0830 generates tech + vol
        if label == "0830":
            rsi, ema20, ema50, ema100, macd, signal, htrend = compute_technicals(close, high, low)
            tech_rows.append([
                sym, rsi, macd.iloc[-1], macd.iloc[-2],
                signal.iloc[-1], signal.iloc[-2], htrend,
                ema20, ema50, ema100
            ])

            std, atr, wow, t13 = compute_vol(close, high, low)
            vol_rows.append([sym, std, atr, wow, t13])

    # Save fundamentals
    fund_path = f"{BASE}/fundamentals_{label}.csv"
    pd.DataFrame(fund_rows, columns=[
        "Symbol","Latest",
        "P1D","P1W","P1M","P3M","P6M","P1Y",
        "R1D","R1W","R1M","R3M","R6M","R1Y"
    ]).to_csv(fund_path, index=False)

    print(f"✔ Saved fundamentals_{label}.csv")

    if label == "0830":
        pd.DataFrame(tech_rows, columns=[
            "Symbol","RSI","MACD_L","MACD_prev",
            "Signal_L","Signal_prev","HistTrend",
            "EMA20","EMA50","EMA100"
        ]).to_csv(tech_0830, index=False)

        pd.DataFrame(vol_rows, columns=[
            "Symbol","WeeklyStd","WeeklyATR","WoW","Trend13W"
        ]).to_csv(vol_0830, index=False)

        print("✔ Saved technicals_0830.csv and volatility_0830.csv")

    return fund_path

# --------------------------------------------------------------
# Comparison generator
# --------------------------------------------------------------
def generate_comparison():
    if not os.path.exists(fund_0830) or not os.path.exists(fund_1530):
        print("⚠ Cannot generate comparison — missing files.")
        return

    df_m = pd.read_csv(fund_0830)
    df_e = pd.read_csv(fund_1530)

    comp = df_m.merge(df_e, on="Symbol", suffixes=("_0830","_1530"))
    out = f"{BASE}/comparison_{date_str}.csv"
    comp.to_csv(out, index=False)
    print(f"📊 Comparison generated: {out}")

# --------------------------------------------------------------
# EXECUTION FLOW BASED ON RUN_MODE
# --------------------------------------------------------------

if run_mode == "0830":
    run_snapshot("0830")

elif run_mode == "1530":
    run_snapshot("1530")
    generate_comparison()

elif run_mode == "BOTH":
    run_snapshot("0830")
    run_snapshot("1530")
    generate_comparison()

elif run_mode == "COMPARE":
    generate_comparison()

else:
    print(f"⚠ Unknown run mode: {run_mode}")

print("🎉 Completed!")

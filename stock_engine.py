import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import os
import pytz

# -------------------------------------------------------
# Setup paths
# -------------------------------------------------------
base_dir = "output"
archive_dir = os.path.join(base_dir, "archive")
os.makedirs(base_dir, exist_ok=True)
os.makedirs(archive_dir, exist_ok=True)

# -------------------------------------------------------
# Load stock list
# -------------------------------------------------------
stocks = pd.read_csv("stocks.csv")["Symbol"].tolist()
stocks = [s.strip().upper() for s in stocks]

# -------------------------------------------------------
# Detect IST time for labeling
# -------------------------------------------------------
utc_now = datetime.now(timezone.utc)
ist = pytz.timezone("Asia/Kolkata")
ist_now = utc_now.astimezone(ist)

HHMM = ist_now.hour * 100 + ist_now.minute

# BEFORE 15:30 IST = morning label
if HHMM < 1530:
    label = "0830"
else:
    label = "1530"

date_str = ist_now.strftime("%Y-%m-%d")

# -------------------------------------------------------
# Determine run type
# -------------------------------------------------------
is_auto_run = "GITHUB_ACTIONS" in os.environ

# -------------------------------------------------------
# Fetcher
# -------------------------------------------------------
def fetch(symbol):
    try:
        df = yf.download(symbol, period="5y", interval="1d", auto_adjust=True, progress=False)
        if df is not None and not df.empty:
            return df
    except:
        pass

    try:
        df = yf.Ticker(symbol).history(period="5y", auto_adjust=True)
        if df is not None and not df.empty:
            return df
    except:
        pass

    return pd.DataFrame()

# -------------------------------------------------------
# Containers
# -------------------------------------------------------
fund = []
tech = []
vol = []

# -------------------------------------------------------
# Loop stocks
# -------------------------------------------------------
for sym in stocks:
    data = fetch(sym)
    if data.empty:
        continue

    close = data["Close"].squeeze()
    high = data["High"].squeeze()
    low = data["Low"].squeeze()

    latest = close.iloc[-1]

    def lag(days):
        return close.iloc[-days] if len(close) > days else np.nan

    # Returns
    def ret(days):
        if len(close) <= days: 
            return np.nan
        return (latest - close.iloc[-days]) / close.iloc[-days] * 100

    fund.append([
        sym, latest,
        lag(1), lag(5), lag(22), lag(66), lag(132), lag(252),
        ret(1), ret(5), ret(22), ret(66), ret(132), ret(252)
    ])

    # RSI
    delta = close.diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    rs = pd.Series(gain).ewm(14).mean() / pd.Series(loss).ewm(14).mean()
    rsi_val = 100 - 100 / (1 + rs.iloc[-1])

    # EMA / MACD
    ema = lambda s, p: s.ewm(span=p, adjust=False).mean()
    ema20 = ema(close, 20).iloc[-1]
    ema50 = ema(close, 50).iloc[-1]
    ema100 = ema(close, 100).iloc[-1]

    ema12 = ema(close, 12)
    ema26 = ema(close, 26)
    macd_line = ema12 - ema26
    signal = ema(macd_line, 9)
    hist = macd_line - signal

    tech.append([
        sym, rsi_val, macd_line.iloc[-1], macd_line.iloc[-2],
        signal.iloc[-1], signal.iloc[-2], hist.iloc[-1] - hist.iloc[-2],
        ema20, ema50, ema100,
        lag(5), lag(22), lag(66)
    ])

    # Volatility
    weekly = close.resample("W").last()
    stddev = weekly.pct_change().std() * 100
    atr = (high - low).resample("W").mean().iloc[-1]
    wow = (weekly.iloc[-1] - weekly.iloc[-2]) / weekly.iloc[-2] * 100 if len(weekly) > 2 else np.nan
    trend13 = (weekly.iloc[-1] - weekly.iloc[-13]) / weekly.iloc[-13] * 100 if len(weekly) > 13 else np.nan

    vol.append([sym, stddev, atr, wow, trend13])

# -------------------------------------------------------
# Save main CSVs
# -------------------------------------------------------
fund_path = f"{base_dir}/fundamentals_{label}.csv"
tech_path = f"{base_dir}/technicals_0830.csv"
vol_path = f"{base_dir}/volatility_0830.csv"

pd.DataFrame(fund, columns=[
    "Symbol","Latest",
    "P_1D","P_1W","P_1M","P_3M","P_6M","P_1Y",
    "R_1D","R_1W","R_1M","R_3M","R_6M","R_1Y"
]).to_csv(fund_path, index=False)

if label == "0830":
    # morning run generates all 3
    pd.DataFrame(tech, columns=[
        "Symbol","RSI","MACD_L","MACD_prev","Signal_L",
        "Signal_prev","HistTrend","EMA20","EMA50","EMA100",
        "P_1W","P_1M","P_3M"
    ]).to_csv(tech_path, index=False)

    pd.DataFrame(vol, columns=[
        "Symbol","WeeklyStd","WeeklyATR","WoW","Trend13W"
    ]).to_csv(vol_path, index=False)

# -------------------------------------------------------
# Comparison file for 1530
# -------------------------------------------------------
if label == "1530":
    try:
        morning_file = f"{base_dir}/fundamentals_0830.csv"
        if os.path.exists(morning_file):
            df1 = pd.read_csv(morning_file)
            df2 = pd.DataFrame(fund, columns=df1.columns)

            comp = df2.merge(df1, on="Symbol", suffixes=("_1530","_0830"))
            comp.to_csv(f"{base_dir}/comparison_1530.csv", index=False)
    except:
        pass

# -------------------------------------------------------
# Auto-run archives only
# -------------------------------------------------------
if is_auto_run:
    run_dir = os.path.join(archive_dir, date_str)
    os.makedirs(run_dir, exist_ok=True)

    os.system(f"cp {fund_path} {run_dir}/")
    if label == "0830":
        os.system(f"cp {tech_path} {run_dir}/")
        os.system(f"cp {vol_path} {run_dir}/")

print(f"Completed run with label {label} IST")

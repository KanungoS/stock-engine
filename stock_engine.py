import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import shutil

# ============================================================
# 0. Ensure main output folders exist
# ============================================================
os.makedirs("output", exist_ok=True)
os.makedirs("output/archive", exist_ok=True)

# ============================================================
# 1. Determine RUN MODE (0830 or 1530 logic)
# ============================================================

now = datetime.now()
current_time_str = now.strftime("%H:%M")
today_str = now.strftime("%Y-%m-%d")

# RULES:
# - Manual run BEFORE 15:30 → treat as 0830
# - Manual run AFTER 15:30 → treat as 1530
# - Auto-run at 0830 → 0830 mode
# - Auto-run at 1530 → 1530 mode

if now.hour < 15 or (now.hour == 15 and now.minute < 30):
    RUN_MODE = "0830"
else:
    RUN_MODE = "1530"

print(f"🕒 Running script in mode: {RUN_MODE}")

# ============================================================
# 2. Build timestamped filenames
# ============================================================

fund_file = f"fundamentals_{RUN_MODE}.csv"
tech_file = f"technicals_{RUN_MODE}.csv"
vol_file = f"volatility_{RUN_MODE}.csv"

# Full versioned archive filenames
fund_file_arch = f"fundamentals_{RUN_MODE}_{today_str}.csv"
tech_file_arch = f"technicals_0830_{today_str}.csv"
vol_file_arch = f"volatility_0830_{today_str}.csv"

# ============================================================
# 3. Load stock list
# ============================================================
stocks = pd.read_csv("stocks.csv")["Symbol"].tolist()
stocks = [s.strip().upper() for s in stocks]
print("Total stocks loaded:", len(stocks))

# ============================================================
# 4. Fetcher Utility (robust yfinance with fallbacks)
# ============================================================
def fetch_data(symbol):
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

    try:
        df = yf.download(symbol,
                         start=(datetime.now() - timedelta(days=1825)).strftime("%Y-%m-%d"),
                         end=datetime.now().strftime("%Y-%m-%d"),
                         interval="1d",
                         auto_adjust=True,
                         progress=False)
        if df is not None and not df.empty:
            return df
    except:
        pass

    return pd.DataFrame()

# ============================================================
# 5. Indicators
# ============================================================
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

# ============================================================
# 6. Output containers
# ============================================================
df_fund = []
df_tech = []
df_vol = []

# ============================================================
# 7. PROCESS EACH STOCK
# ============================================================
for symbol in stocks:
    try:
        print(f"Processing: {symbol}")

        data = fetch_data(symbol)
        if data is None or data.empty:
            print(f"⚠ WARNING: No data for {symbol} — skipping")
            continue

        close = data["Close"].squeeze()
        high  = data["High"].squeeze()
        low   = data["Low"].squeeze()

        # ----------- Historical prices ----------- 
        latest = close.iloc[-1]
        price_1D = close.iloc[-2] if len(close) >= 2 else np.nan
        price_1W = close.iloc[-5] if len(close) >= 5 else np.nan
        price_1M = close.iloc[-22] if len(close) >= 22 else np.nan
        price_3M = close.iloc[-66] if len(close) >= 66 else np.nan
        price_6M = close.iloc[-132] if len(close) >= 132 else np.nan
        price_1Y = close.iloc[-252] if len(close) >= 252 else np.nan

        # ----------- Returns (INDMoney-style) -----------
        return_1D = (latest - price_1D) / price_1D * 100 if pd.notna(price_1D) else np.nan
        return_1W = (latest - price_1W) / price_1W * 100 if pd.notna(price_1W) else np.nan
        return_1M = (latest - price_1M) / price_1M * 100 if pd.notna(price_1M) else np.nan
        return_3M = (latest - price_3M) / price_3M * 100 if pd.notna(price_3M) else np.nan
        return_6M = (latest - price_6M) / price_6M * 100 if pd.notna(price_6M) else np.nan
        return_1Y = (latest - price_1Y) / price_1Y * 100 if pd.notna(price_1Y) else np.nan

        # ----------- RSI ----------
        rsi_val = rsi(close).iloc[-1]

        # ----------- EMA ----------
        ema20 = ema(close, 20).iloc[-1]
        ema50 = ema(close, 50).iloc[-1]
        ema100 = ema(close, 100).iloc[-1]

        # ----------- MACD ----------
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

        # ----------- Volatility -----------
        weekly = close.resample("W").last()
        weekly_stddev = weekly.pct_change().std() * 100
        weekly_atr = (high - low).resample("W").mean().iloc[-1]
        weekly_wow = ((weekly.iloc[-1] - weekly.iloc[-2]) / weekly.iloc[-2] * 100) if len(weekly) >= 2 else np.nan
        trend_13w = ((weekly.iloc[-1] - weekly.iloc[-13]) / weekly.iloc[-13] * 100) if len(weekly) >= 13 else np.nan

        # ----------- Append Fundamentals -----------
        df_fund.append([
            symbol, latest, price_1D, price_1W, price_1M,
            price_3M, price_6M, price_1Y,
            return_1D, return_1W, return_1M,
            return_3M, return_6M, return_1Y
        ])

        # ----------- Append Technicals -----------
        df_tech.append([
            symbol, rsi_val, macd_latest, macd_1d, signal_latest,
            signal_1d, histogram_trend, ema20, ema50, ema100,
            price_1W, price_1M, price_3M
        ])

        # ----------- Append Volatility -----------
        df_vol.append([
            symbol, weekly_stddev, weekly_atr, weekly_wow, trend_13w
        ])

    except Exception as e:
        print(f"❌ Error for {symbol}: {e}")
        continue
# ============================================================
# 8. Convert to DataFrames & SAVE OUTPUT FILES
# ============================================================

df_fund = pd.DataFrame(df_fund, columns=[
    "Symbol", "Latest_Close",
    "Price_1D", "Price_1W", "Price_1M", "Price_3M", "Price_6M", "Price_1Y",
    "Return_1D", "Return_1W", "Return_1M", "Return_3M", "Return_6M", "Return_1Y"
])

df_tech = pd.DataFrame(df_tech, columns=[
    "Symbol", "RSI", "MACD_Latest", "MACD_1D", "Signal_Latest",
    "Signal_1D", "Histogram_Trend", "EMA20", "EMA50", "EMA100",
    "Price_1W", "Price_1M", "Price_3M"
])

df_vol = pd.DataFrame(df_vol, columns=[
    "Symbol", "Weekly_StdDev", "Weekly_ATR", "Weekly_WoW", "Trend_13W"
])


# ============================================================
# 9. SAVE OUTPUT TO main /output folder
# ============================================================

df_fund.to_csv(f"output/{fund_file}", index=False)
print(f"✔ Fundamentals saved to output/{fund_file}")

# Important rule: technicals & volatility only regenerate at 0830
if RUN_MODE == "0830":
    df_tech.to_csv(f"output/{tech_file}", index=False)
    df_vol.to_csv(f"output/{vol_file}", index=False)

    print(f"✔ Technicals saved to output/{tech_file}")
    print(f"✔ Volatility saved to output/{vol_file}")
else:
    print("ℹ 15:30 run: Technicals & Volatility NOT regenerated (reuse morning versions)")


# ============================================================
# 10. AUTO-ARCHIVE DAILY VERSIONED FILES
# ============================================================

archive_path = f"output/archive/{today_str}"
os.makedirs(archive_path, exist_ok=True)

# Save fundamentals always
df_fund.to_csv(f"{archive_path}/{fund_file_arch}", index=False)

# Save tech + vol only at 0830
if RUN_MODE == "0830":
    df_tech.to_csv(f"{archive_path}/{tech_file_arch}", index=False)
    df_vol.to_csv(f"{archive_path}/{vol_file_arch}", index=False)

print(f"📦 Archived in: output/archive/{today_str}")


# ============================================================
# 11. BUILD DAILY SUMMARY DASHBOARD
# ============================================================

summary_file = f"output/summary_{today_str}.csv"

fund_0830 = f"output/archive/{today_str}/fundamentals_0830_{today_str}.csv"
fund_1530 = f"output/archive/{today_str}/fundamentals_1530_{today_str}.csv"

df_dash = None

if os.path.exists(fund_0830) and os.path.exists(fund_1530):

    df_a = pd.read_csv(fund_0830).add_suffix("_0830")
    df_b = pd.read_csv(fund_1530).add_suffix("_1530")

    # Fix symbol duplication
    df_a.rename(columns={"Symbol_0830": "Symbol"}, inplace=True)
    df_b.rename(columns={"Symbol_1530": "Symbol"}, inplace=True)

    df_dash = pd.merge(df_a, df_b, on="Symbol", how="outer")
    df_dash.to_csv(summary_file, index=False)

    print(f"📊 Summary dashboard generated → {summary_file}")

else:
    print("ℹ Summary will generate after BOTH 08:30 & 15:30 runs are available.")


# ============================================================
# 12. CLEAN END
# ============================================================

print("\n🎉 SUCCESS — All tasks completed with time logic, archive, versioning, dashboard!\n")

import pandas as pd
import yfinance as yf
import numpy as np
import datetime

# ===============================================================
# CONFIG
# ===============================================================
LOOKBACK_DAYS = 400   # ensures enough data for all indicators
WEEKS = 13            # for volatility

# ===============================================================
# LOAD STOCK LIST
# ===============================================================
stocks_df = pd.read_csv("stocks.csv")
symbols = stocks_df["Symbol"].tolist()

# ===============================================================
# CONTAINERS FOR 4 OUTPUT FILES
# ===============================================================
fundamental_rows = []
technical_rows = []
volatility_rows = []
combined_rows = []

# ===============================================================
# PROCESS EACH STOCK
# ===============================================================
for sym in symbols:
    print(f"Processing {sym} ...")

    try:
        ticker = yf.Ticker(sym)
        hist = ticker.history(period=f"{LOOKBACK_DAYS}d")

        if hist.empty:
            print(f"⚠ No data for {sym}")
            continue

        hist = hist.dropna()

        close = hist["Close"]

        # ======================================================
        # FUNDAMENTALS
        # ======================================================
        price_latest = close.iloc[-1]
        price_1d = close.iloc[-2] if len(close) >= 2 else np.nan
        price_5d = close.iloc[-6] if len(close) >= 6 else np.nan
        price_1m = close.iloc[-22] if len(close) >= 22 else np.nan
        price_3m = close.iloc[-66] if len(close) >= 66 else np.nan
        price_6m = close.iloc[-132] if len(close) >= 132 else np.nan
        price_1y = close.iloc[0]

        def ret(now, old):
            return ((now - old) / old) * 100 if old and old > 0 else np.nan

        fundamental_rows.append({
            "Symbol": sym,
            "Price_Latest": price_latest,
            "Price_1D": price_1d,
            "Price_5D": price_5d,
            "Price_1M": price_1m,
            "Price_3M": price_3m,
            "Price_6M": price_6m,
            "Price_1Y": price_1y,
            "Return_1D": ret(price_latest, price_1d),
            "Return_5D": ret(price_latest, price_5d),
            "Return_1M": ret(price_latest, price_1m),
            "Return_3M": ret(price_latest, price_3m),
            "Return_6M": ret(price_latest, price_6m),
            "Return_1Y": ret(price_latest, price_1y)
        })

        # ======================================================
        # TECHNICAL INDICATORS
        # ======================================================
        df_t = pd.DataFrame()
        df_t["close"] = close

        df_t["RSI"] = ta.rsi(df_t["close"], length=14)
        macd = ta.macd(df_t["close"])
        df_t["MACD"] = macd["MACD_12_26_9"]
        df_t["Signal"] = macd["MACDs_12_26_9"]

        df_t["EMA20"] = ta.ema(df_t["close"], length=20)
        df_t["EMA50"] = ta.ema(df_t["close"], length=50)
        df_t["EMA100"] = ta.ema(df_t["close"], length=100)

        def safe(df, offset):
            return df.iloc[-offset] if len(df) >= offset else np.nan

        RSI_L = safe(df_t["RSI"], 1)
        RSI_1D = safe(df_t["RSI"], 2)
        RSI_5D = safe(df_t["RSI"], 6)

        MACD_L = safe(df_t["MACD"], 1)
        MACD_1D = safe(df_t["MACD"], 2)
        MACD_5D = safe(df_t["MACD"], 6)

        SIG_L = safe(df_t["Signal"], 1)
        SIG_1D = safe(df_t["Signal"], 2)
        SIG_5D = safe(df_t["Signal"], 6)

        # Histogram Trend = (MACD_L - SIG_L) - (MACD_1D - SIG_1D)
        hist_L = MACD_L - SIG_L if MACD_L is not np.nan else np.nan
        hist_Y = MACD_1D - SIG_1D if MACD_1D is not np.nan else np.nan
        hist_trend = hist_L - hist_Y if hist_L is not np.nan else np.nan

        # Price – EMA difference
        Price_20 = price_latest - safe(df_t["EMA20"], 1)
        Price_50 = price_latest - safe(df_t["EMA50"], 1)
        Price_100 = price_latest - safe(df_t["EMA100"], 1)

        technical_rows.append({
            "Symbol": sym,
            "RSI_Latest": RSI_L,
            "MACD_Latest": MACD_L,
            "Signal_Latest": SIG_L,
            "Histogram_Trend": hist_trend,
            "RSI_1D": RSI_1D,
            "MACD_1D": MACD_1D,
            "Signal_1D": SIG_1D,
            "RSI_5D": RSI_5D,
            "MACD_5D": MACD_5D,
            "Signal_5D": SIG_5D,
            "EMA20": safe(df_t["EMA20"], 1),
            "EMA50": safe(df_t["EMA50"], 1),
            "EMA100": safe(df_t["EMA100"], 1),
            "Price_20": Price_20,
            "Price_50": Price_50,
            "Price_100": Price_100
        })

        # ======================================================
        # WEEKLY VOLATILITY (13 Weeks)
        # ======================================================
        weekly = hist["Close"].resample("W-FRI").last().dropna()
        if len(weekly) >= WEEKS + 1:
            wk = weekly[-(WEEKS+1):]
            wow_pct = wk.pct_change() * 100

            std_13w = wow_pct.std()
            atr_13w = (wk.diff().abs().mean() / price_latest) * 100
            last_wow = wow_pct.iloc[-1]

            trend_13w = (wow_pct.iloc[-3:].sum()) / (wow_pct.mean())

            volatility_rows.append({
                "Symbol": sym,
                "STD_13W_Pct": std_13w,
                "ATR_13W_Pct": atr_13w,
                "WOW_Pct": last_wow,
                "Trend_13W": trend_13w
            })

        # ======================================================
        # COMBINED ROW (MERGE EVERYTHING)
        # ======================================================
        combined_rows.append({
            **fundamental_rows[-1],
            **technical_rows[-1],
        except Exception as e:
        print(f"❌ ERROR: {sym} - {e}")

# ===============================================================
# EXPORT ALL FILES
# ===============================================================
pd.DataFrame(fundamental_rows).to_csv("fundamental_scores.csv", index=False)
pd.DataFrame(technical_rows).to_csv("technical_scores.csv", index=False)
pd.DataFrame(volatility_rows).to_csv("weekly_volatility.csv", index=False)
pd.DataFrame(combined_rows).to_csv("master_scores.csv", index=False)

print("✔ ALL CSV FILES GENERATED SUCCESSFULLY!")



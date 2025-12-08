import yfinance as yf
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime
from tqdm import tqdm

MASTER_SHEET_PATH = "master_sheet.xlsx"
CACHE_FOLDER = "cache"
os.makedirs(CACHE_FOLDER, exist_ok=True)


# ---------------------------------------------------
# Utility Functions
# ---------------------------------------------------
def safe_float(val):
    try:
        return float(val)
    except:
        return np.nan


def compute_rsi(prices, period=14):
    delta = prices.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def compute_macd(series):
    ema12 = series.ewm(span=12, adjust=False).mean()
    ema26 = series.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    hist = macd - signal
    return macd, signal, hist


# ---------------------------------------------------
# Load Symbols (No Header)
# ---------------------------------------------------
def load_symbols():
    df = pd.read_csv("stocks.csv", header=None)

    symbols = (
        df[0]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )

    # Remove junk header-like entries
    symbols = [s for s in symbols if s.upper() not in ["SYMBOL", "SYMBO1"]]

    print(f"Total symbols loaded: {len(symbols)}")
    print("Sample tickers:", symbols[:10])

    return symbols


# ---------------------------------------------------
# Stock Download with Cache
# ---------------------------------------------------
def fetch_stock_data(symbol):
    cache_file = os.path.join(CACHE_FOLDER, f"{symbol}.csv")

    # Use cache if exists
    if os.path.exists(cache_file):
        try:
            df = pd.read_csv(cache_file)
            if not df.empty:
                return df
        except:
            pass

    # Download fresh
    for _ in range(3):  # retry 3 times
        try:
            df = yf.download(symbol, period="5y", interval="1d", progress=False)
            if not df.empty:
                df.to_csv(cache_file, index=True)
                return df
        except Exception:
            time.sleep(1)

    return pd.DataFrame()  # failed


# ---------------------------------------------------
# Build Master Sheet
# ---------------------------------------------------
def build_master_sheet():
    symbols = load_symbols()
    master_rows = []
    valid_rows = 0

    pbar = tqdm(symbols, desc="Processing", ncols=100)

    for symbol in pbar:
        df = fetch_stock_data(symbol)

        if df.empty:
            continue

        df["Return"] = df["Close"].pct_change()
        latest_close = safe_float(df["Close"].iloc[-1])

        try:
            rsi = compute_rsi(df["Close"]).iloc[-1]
        except:
            rsi = np.nan

        try:
            macd, signal, hist = compute_macd(df["Close"])
            macd_val = macd.iloc[-1]
            signal_val = signal.iloc[-1]
            hist_val = hist.iloc[-1]
        except:
            macd_val = signal_val = hist_val = np.nan

        # returns
        def get_prev(days):
            if len(df) > days:
                return safe_float(df["Close"].iloc[-days])
            return np.nan

        row = {
            "Symbol": symbol,
            "Latest Close": latest_close,
            "Close_1W": get_prev(5),
            "Close_1M": get_prev(22),
            "Close_3M": get_prev(66),
            "Close_6M": get_prev(132),
            "Close_1Y": get_prev(260),
            "Close_2Y": get_prev(520),
            "Close_3Y": get_prev(780),
            "Close_4Y": get_prev(1040),
            "Close_5Y": get_prev(1300),
            "RSI": rsi,
            "MACD": macd_val,
            "Signal": signal_val,
            "Histogram": hist_val
        }

        master_rows.append(row)
        valid_rows += 1

    master_df = pd.DataFrame(master_rows)

    if master_df.empty:
        print("\nWARNING: No valid data downloaded for any symbol.")
        print("This could be due to temporary Yahoo issues or bad tickers.")
        master_df.to_excel(MASTER_SHEET_PATH, index=False)
        return master_df

    master_df.to_excel(MASTER_SHEET_PATH, index=False)
    print("\nMaster sheet created successfully.")
    return master_df


# ---------------------------------------------------
# Main
# ---------------------------------------------------
if __name__ == "__main__":
    build_master_sheet()

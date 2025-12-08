# master_engine.py

import os
import time
import warnings
from datetime import datetime
import numpy as np
import pandas as pd
import yfinance as yf
from tqdm import tqdm

warnings.filterwarnings("ignore")

STOCK_LIST_FILE = "stocks.csv"   # one symbol per line, without .NS
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def ensure_symbol_ns(sym: str) -> str:
    sym = str(sym).strip().upper()
    if not sym.endswith(".NS"):
        sym += ".NS"
    return sym


def load_stock_list():
    df = pd.read_csv(STOCK_LIST_FILE, header=None)
    symbols = [ensure_symbol_ns(s) for s in df[0].tolist() if str(s).strip()]
    return symbols


def safe_price(df: pd.DataFrame, days: int):
    try:
        return float(df["Close"].iloc[-days])
    except Exception:
        return np.nan


def get_rsi(df: pd.DataFrame, period: int = 14):
    try:
        close = df["Close"]
        if len(close) < period + 1:
            return np.nan
        delta = close.diff()
        gain = np.where(delta > 0, delta, 0.0)
        loss = np.where(delta < 0, -delta, 0.0)
        gain_ema = pd.Series(gain).ewm(span=period, adjust=False).mean()
        loss_ema = pd.Series(loss).ewm(span=period, adjust=False).mean()
        rs = gain_ema.iloc[-1] / loss_ema.iloc[-1] if loss_ema.iloc[-1] != 0 else np.nan
        if np.isnan(rs):
            return np.nan
        return 100 - (100 / (1 + rs))
    except Exception:
        return np.nan


def get_macd(df: pd.DataFrame):
    try:
        close = df["Close"]
        if len(close) < 35:
            return np.nan, np.nan, np.nan
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal = macd_line.ewm(span=9, adjust=False).mean()
        hist = macd_line - signal
        return float(macd_line.iloc[-1]), float(signal.iloc[-1]), float(hist.iloc[-1])
    except Exception:
        return np.nan, np.nan, np.nan


def download_history(symbol: str):
    for _ in range(2):
        try:
            df = yf.download(
                symbol,
                period="5y",
                interval="1d",
                auto_adjust=True,
                progress=False,
            )
            if df is not None and not df.empty:
                df = df.reset_index()
                if "Date" in df.columns:
                    df["Date"] = pd.to_datetime(df["Date"])
                    df = df.sort_values("Date").set_index("Date")
                df = df.dropna(subset=["Close"])
                if not df.empty:
                    return df
        except Exception:
            pass
        time.sleep(0.3)
    return None

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def build_master_sheet():
    symbols = load_stock_list()
    records = []

    print(f"Total symbols: {len(symbols)}")

    pbar = tqdm(symbols, desc="Processing", ncols=90)
    start = time.time()

    for sym in pbar:
        df = download_history(sym)
        if df is None or df.empty:
            continue

        last_close = float(df["Close"].iloc[-1])

        rec = {
            "symbol": sym,
            "last_close": last_close,
            "price_1w_ago": safe_price(df, 5),
            "price_1m_ago": safe_price(df, 21),
            "price_3m_ago": safe_price(df, 63),
            "price_6m_ago": safe_price(df, 126),
            "price_1y_ago": safe_price(df, 252),
            "price_2y_ago": safe_price(df, 504),
            "price_3y_ago": safe_price(df, 756),
            "price_4y_ago": safe_price(df, 1008),
            "price_5y_ago": safe_price(df, 1260),
        }

        # returns %
        for key, days in [
            ("ret_1w_pct", 5),
            ("ret_1m_pct", 21),
            ("ret_3m_pct", 63),
            ("ret_6m_pct", 126),
            ("ret_1y_pct", 252),
            ("ret_2y_pct", 504),
            ("ret_3y_pct", 756),
            ("ret_4y_pct", 1008),
            ("ret_5y_pct", 1260),
        ]:
            past = safe_price(df, days)
            if np.isnan(past) or past == 0:
                rec[key] = np.nan
            else:
                rec[key] = (last_close - past) / past * 100.0

        rec["rsi_14"] = get_rsi(df, period=14)
        macd_line, macd_signal, macd_hist = get_macd(df)
        rec["macd_line"] = macd_line
        rec["macd_signal"] = macd_signal
        rec["macd_hist"] = macd_hist

        records.append(rec)

        done = pbar.n + 1
        ratio = done / len(symbols)
        elapsed = time.time() - start
        if ratio > 0:
            eta = elapsed / ratio - elapsed
            pbar.set_postfix({"ETA(min)": f"{eta/60:.1f}"})

    master_df = pd.DataFrame(records)
    if master_df.empty:
        raise RuntimeError("No valid data downloaded for any symbol.")

    master_df = master_df.sort_values("symbol").reset_index(drop=True)
    out_path = os.path.join(OUTPUT_DIR, "master_sheet.csv")
    master_df.to_csv(out_path, index=False)
    print(f"Master sheet saved to {out_path}")


if __name__ == "__main__":
    build_master_sheet()

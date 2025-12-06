import os
import time
import json
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from tqdm import tqdm

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
STOCK_LIST_FILE = "stocks.csv"                  # One symbol per line, without .NS extension
CACHE_DIR = "cache"
OUTPUT_DIR = "output"

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Retry logic
MAX_RETRIES = 2

# Weights for scoring
WEIGHTS_SHORT = {"1w": 0.45, "1m": 0.40, "rsi": 0.15}
WEIGHTS_MEDIUM = {"3m": 0.40, "6m": 0.35, "1y": 0.25}
WEIGHTS_LONG = {"2y": 0.30, "3y": 0.30, "5y": 0.40}
WEIGHTS_STAR = {"long": 0.40, "medium": 0.35, "short": 0.25}


# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def pct_change(df, days):
    """Compute % change N days ago to last close."""
    try:
        past = df['Close'].iloc[-days]
        last = df['Close'].iloc[-1]
        return (last - past) / past * 100
    except:
        return np.nan


def get_rsi(df, period=14):
    delta = df['Close'].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    gain_ema = pd.Series(gain).ewm(span=period, adjust=False).mean()
    loss_ema = pd.Series(loss).ewm(span=period, adjust=False).mean()
    rs = gain_ema / loss_ema
    return 100 - (100 / (1 + rs.iloc[-1]))


def load_stock_list():
    df = pd.read_csv(STOCK_LIST_FILE, header=None)
    return [x.strip() + ".NS" for x in df[0].tolist()]


def load_cached(symbol):
    f = os.path.join(CACHE_DIR, f"{symbol}.csv")
    if os.path.exists(f):
        try:
            return pd.read_csv(f, parse_dates=["Date"], index_col="Date")
        except:
            return None
    return None


def save_cached(symbol, df):
    df.to_csv(os.path.join(CACHE_DIR, f"{symbol}.csv"))


def download_data(symbol):
    """Download with caching + retry."""
    df = load_cached(symbol)
    if df is not None and len(df) > 0:
        return df

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = yf.download(symbol, period="5y", interval="1d", progress=False)
            if len(df) > 0:
                df = df.reset_index()
                save_cached(symbol, df)
                return df
        except Exception as e:
            if attempt == MAX_RETRIES:
                return None
        time.sleep(1)

    return None


# ------------------------------------------------------------
# SCORE CALCULATIONS
# ------------------------------------------------------------
def compute_scores(df):
    results = {}
    # Price changes
    results["1w"] = pct_change(df, 5)
    results["1m"] = pct_change(df, 21)
    results["3m"] = pct_change(df, 63)
    results["6m"] = pct_change(df, 126)
    results["1y"] = pct_change(df, 252)
    results["2y"] = pct_change(df, 504)
    results["3y"] = pct_change(df, 756)
    results["5y"] = pct_change(df, 1260)

    # RSI
    results["rsi"] = get_rsi(df)

    return results


def weighted_score(values, weights):
    score = 0
    for k, w in weights.items():
        if k in values and pd.notna(values[k]):
            score += values[k] * w
    return score


# ------------------------------------------------------------
# MAIN ENGINE
# ------------------------------------------------------------
def run_engine(mode):
    stocks = load_stock_list()
    records = []
    failed_stocks = []

    print(f"\nRunning mode: {mode.upper()}")
    print(f"Total stocks: {len(stocks)}")

    start_time = time.time()
    pbar = tqdm(stocks, desc="Processing", ncols=90)

    for symbol in pbar:
        df = download_data(symbol)
        if df is None:
            failed_stocks.append(symbol)
            continue

        scores = compute_scores(df)

        rec = {"symbol": symbol}
        rec.update(scores)
        rec["score_short"] = weighted_score(scores, WEIGHTS_SHORT)
        rec["score_medium"] = weighted_score(scores, WEIGHTS_MEDIUM)
        rec["score_long"] = weighted_score(scores, WEIGHTS_LONG)

        # Calculate progress ETA
        elapsed = time.time() - start_time
        done_ratio = (pbar.n + 1) / len(stocks)
        if done_ratio > 0:
            eta = elapsed / done_ratio - elapsed
            pbar.set_postfix({"ETA (min)": f"{eta/60:.1f}"})

        records.append(rec)

    # Remove stocks that failed twice from further calculations
    if mode == "master":
        print(f"\nFailed stocks removed: {len(failed_stocks)}")

    df = pd.DataFrame(records)

    # MASTER MODE → Write full sheet
    if mode == "master":
        df.to_csv(os.path.join(OUTPUT_DIR, "master_sheet.csv"), index=False)
        print("Master sheet created.")
        return

    # Scenario modes
    if mode == "short":
        df = df.sort_values("score_short", ascending=False).head(20)
        df.to_csv(os.path.join(OUTPUT_DIR, "top20_short.csv"), index=False)

    elif mode == "medium":
        df = df.sort_values("score_medium", ascending=False).head(20)
        df.to_csv(os.path.join(OUTPUT_DIR, "top20_medium.csv"), index=False)

    elif mode == "long":
        df = df.sort_values("score_long", ascending=False).head(20)
        df.to_csv(os.path.join(OUTPUT_DIR, "top20_long.csv"), index=False)

    elif mode == "star":
        # Requires long, medium, short scores
        df["star_score"] = (
            df["score_long"] * WEIGHTS_STAR["long"]
            + df["score_medium"] * WEIGHTS_STAR["medium"]
            + df["score_short"] * WEIGHTS_STAR["short"]
        )
        df = df.sort_values("star_score", ascending=False).head(5)
        df.to_csv(os.path.join(OUTPUT_DIR, "star_top5.csv"), index=False)

    print(f"{mode.upper()} output created.")


# ------------------------------------------------------------
# CLI ENTRY
# ------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, required=True,
                        choices=["master", "short", "medium", "long", "star"])
    args = parser.parse_args()

    run_engine(args.mode)

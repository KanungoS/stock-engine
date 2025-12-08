import os
import time
import warnings
import pandas as pd
import numpy as np
import yfinance as yf
from tqdm import tqdm

# Silence all warnings to avoid GitHub Actions log overflow
warnings.filterwarnings("ignore")

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
STOCK_LIST_FILE = "stocks.csv"
CACHE_DIR = "cache"
OUTPUT_DIR = "output"

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

MAX_RETRIES = 2

# Scoring weights
WEIGHTS_SHORT = {"1w": 0.45, "1m": 0.40, "rsi": 0.15}
WEIGHTS_MEDIUM = {"3m": 0.40, "6m": 0.35, "1y": 0.25}
WEIGHTS_LONG = {"2y": 0.30, "3y": 0.30, "5y": 0.40}
WEIGHTS_STAR = {"long": 0.40, "medium": 0.35, "short": 0.25}

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def pct_change(df, days):
    try:
        past = df["Close"].iloc[-days]
        last = df["Close"].iloc[-1]
        return (last - past) / past * 100
    except:
        return np.nan


def get_rsi(df, period=14):
    try:
        delta = df["Close"].diff()
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        gain_ema = pd.Series(gain).ewm(span=period, adjust=False).mean()
        loss_ema = pd.Series(loss).ewm(span=period, adjust=False).mean()
        rs = gain_ema.iloc[-1] / loss_ema.iloc[-1]
        return 100 - (100 / (1 + rs))
    except:
        return np.nan


def load_stock_list():
    df = pd.read_csv(STOCK_LIST_FILE, header=None)
    return [str(x).strip() + ".NS" for x in df[0].tolist()]


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
    """
    Silent download with caching + retry.
    Prevents log spam, avoids job termination on GitHub.
    """
    df = load_cached(symbol)
    if df is not None and len(df) > 0:
        return df

    for attempt in range(MAX_RETRIES):
        try:
            df = yf.download(
                symbol,
                period="5y",
                interval="1d",
                progress=False,
                auto_adjust=True
            )
            if df is not None and len(df) > 0:
                df = df.reset_index()
                save_cached(symbol, df)
                return df
        except:
            pass
        time.sleep(0.3)

    return None  # silently return None for delisted symbols


def compute_scores(df):
    results = {}
    results["1w"] = pct_change(df, 5)
    results["1m"] = pct_change(df, 21)
    results["3m"] = pct_change(df, 63)
    results["6m"] = pct_change(df, 126)
    results["1y"] = pct_change(df, 252)
    results["2y"] = pct_change(df, 504)
    results["3y"] = pct_change(df, 756)
    results["5y"] = pct_change(df, 1260)
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

    print(f"\nRunning mode: {mode.upper()} | Total stocks: {len(stocks)}\n")

    start_time = time.time()
    pbar = tqdm(stocks, desc="Processing", ncols=90)

    for symbol in pbar:
        df = download_data(symbol)
        if df is None:
            continue  # silently skip missing symbols

        scores = compute_scores(df)

        rec = {"symbol": symbol}
        rec.update(scores)

        # Ensure score fields always exist
        rec["score_short"] = weighted_score(scores, WEIGHTS_SHORT)
        rec["score_medium"] = weighted_score(scores, WEIGHTS_MEDIUM)
        rec["score_long"] = weighted_score(scores, WEIGHTS_LONG)

        # ETA update
        done = pbar.n + 1
        ratio = done / len(stocks)
        elapsed = time.time() - start_time
        if ratio > 0:
            eta = elapsed / ratio - elapsed
            pbar.set_postfix({"ETA(min)": f"{eta/60:.1f}"})

        records.append(rec)

    df = pd.DataFrame(records)

    # ------------------------------------------------------------
    # UNIVERSAL SAFE-GUARD: If df empty → create empty CSV & exit
    # ------------------------------------------------------------
    def safe_empty_output(filename):
        pd.DataFrame().to_csv(os.path.join(OUTPUT_DIR, filename), index=False)
        print(f"{mode.upper()} completed (NO VALID STOCKS).")

    # ------------------------------------------------------------
    # MASTER SHEET
    # ------------------------------------------------------------
    if mode == "master":
        if df.empty:
            safe_empty_output("master_sheet.csv")
            return

        df.to_csv(os.path.join(OUTPUT_DIR, "master_sheet.csv"), index=False)
        print("MASTER sheet created.")
        return

    # ------------------------------------------------------------
    # SHORT MODE
    # ------------------------------------------------------------
    if mode == "short":
        if df.empty or "score_short" not in df.columns:
            safe_empty_output("top20_short.csv")
            return

        df = df.dropna(subset=["score_short"])
        if df.empty:
            safe_empty_output("top20_short.csv")
            return

        df = df.sort_values("score_short", ascending=False).head(20)
        df.to_csv(os.path.join(OUTPUT_DIR, "top20_short.csv"), index=False)
        print("SHORT output created.")
        return

    # ------------------------------------------------------------
    # MEDIUM MODE
    # ------------------------------------------------------------
    if mode == "medium":
        if df.empty or "score_medium" not in df.columns:
            safe_empty_output("top20_medium.csv")
            return

        df = df.dropna(subset=["score_medium"])
        if df.empty:
            safe_empty_output("top20_medium.csv")
            return

        df = df.sort_values("score_medium", ascending=False).head(20)
        df.to_csv(os.path.join(OUTPUT_DIR, "top20_medium.csv"), index=False)
        print("MEDIUM output created.")
        return

    # ------------------------------------------------------------
    # LONG MODE
    # ------------------------------------------------------------
    if mode == "long":
        if df.empty or "score_long" not in df.columns:
            safe_empty_output("top20_long.csv")
            return

        df = df.dropna(subset=["score_long"])
        if df.empty:
            safe_empty_output("top20_long.csv")
            return

        df = df.sort_values("score_long", ascending=False).head(20)
        df.to_csv(os.path.join(OUTPUT_DIR, "top20_long.csv"), index=False)
        print("LONG output created.")
        return

    # ------------------------------------------------------------
    # STAR MODE
    # ------------------------------------------------------------
    if mode == "star":
        needed = ["score_short", "score_medium", "score_long"]
        if df.empty or any(col not in df.columns for col in needed):
            safe_empty_output("star_top5.csv")
            return

        df = df.dropna(subset=needed, how="all")
        if df.empty:
            safe_empty_output("star_top5.csv")
            return

        df["star_score"] = (
            df["score_long"] * WEIGHTS_STAR["long"]
            + df["score_medium"] * WEIGHTS_STAR["medium"]
            + df["score_short"] * WEIGHTS_STAR["short"]
        )

        df = df.sort_values("star_score", ascending=False).head(5)
        df.to_csv(os.path.join(OUTPUT_DIR, "star_top5.csv"), index=False)
        print("STAR output created.")
        return


# ------------------------------------------------------------
# CLI ENTRYPOINT
# ------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        required=True,
        choices=["master", "short", "medium", "long", "star"],
        help="Which segment to run"
    )
    args = parser.parse_args()

    run_engine(args.mode)

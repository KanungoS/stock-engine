import yfinance as yf
import pandas as pd
import numpy as np
import os
import datetime
import json
from pathlib import Path

RAW_DATA_DIR = "raw_data"
FAIL_LOG = "fail_log.json"

Path(RAW_DATA_DIR).mkdir(exist_ok=True)

# ------------------------------------------------------------
# Load failure log
# ------------------------------------------------------------
if os.path.exists(FAIL_LOG):
    with open(FAIL_LOG, "r") as f:
        fail_log = json.load(f)
else:
    fail_log = {}

# ------------------------------------------------------------
# Failure handling
# ------------------------------------------------------------
def record_failure(stock):
    if stock not in fail_log:
        fail_log[stock] = 1
    else:
        fail_log[stock] += 1
    with open(FAIL_LOG, "w") as f:
        json.dump(fail_log, f, indent=2)

def is_removed(stock):
    return fail_log.get(stock, 0) >= 2

# ------------------------------------------------------------
# Download stock data
# ------------------------------------------------------------
def get_stock_data(ticker):
    try:
        data = yf.download(ticker, period="5y", interval="1d", progress=False)
        if data.empty:
            record_failure(ticker)
            return None

        data.to_csv(f"{RAW_DATA_DIR}/{ticker}.csv")
        return data

    except:
        record_failure(ticker)
        return None

# ------------------------------------------------------------
# Percentage change calculator (SAFE)
# ------------------------------------------------------------
def calculate_scores(df):
    try:
        df = df["Close"]
        today = df.iloc[-1]

        def pct(days):
            try:
                if len(df) > days:
                    return float((today - df.iloc[-days]) / df.iloc[-days] * 100)
                return 0.0
            except:
                return 0.0

        return {
            "price_today": float(today),
            "1w": pct(5),
            "1m": pct(21),
            "3m": pct(63),
            "6m": pct(126),
            "1y": pct(252),
            "2y": pct(504),
            "3y": pct(756),
            "4y": pct(1008),
            "5y": pct(1260),
        }
    except:
        return None

# ------------------------------------------------------------
# Scenario scoring
# ------------------------------------------------------------
def scenario_score(row, scenario):
    try:
        if scenario == "short":
            return float(
                (row["1w"] * 0.40) +
                (row["1m"] * 0.35) +
                (row["3m"] * 0.25)
            )
        elif scenario == "medium":
            return float(
                (row["3m"] * 0.30) +
                (row["6m"] * 0.35) +
                (row["1y"] * 0.35)
            )
        else:  # long
            return float(
                (row["1y"] * 0.25) +
                (row["2y"] * 0.25) +
                (row["3y"] * 0.25) +
                (row["5y"] * 0.25)
            )
    except:
        return 0.0

# ------------------------------------------------------------
# Star score
# ------------------------------------------------------------
def star_score(row):
    try:
        weights = {
            "1w": 0.10, "1m": 0.10, "3m": 0.15,
            "6m": 0.20, "1y": 0.20,
            "2y": 0.10, "3y": 0.10, "5y": 0.05
        }
        total = 0.0
        for k, w in weights.items():
            total += float(row[k]) * w
        return float(total)
    except:
        return 0.0

# ------------------------------------------------------------
# Time remaining predictor
# ------------------------------------------------------------
def estimate_time_remaining(start_time, progress, total):
    elapsed = (datetime.datetime.now() - start_time).seconds
    if progress == 0:
        return "Calculating..."
    est_total = elapsed / (progress / total)
    remaining = est_total - elapsed
    return f"{int(remaining)} sec remaining"

# ------------------------------------------------------------
# Main Engine
# ------------------------------------------------------------
def run_engine(stock_list):
    master = []
    start_time = datetime.datetime.now()

    total = len(stock_list)
    processed = 0

    for stock in stock_list:
        processed += 1

        if is_removed(stock):
            print(f"Skipping {stock}: auto-disabled.")
            continue

        print(f"{processed}/{total} → Processing {stock} | "
              f"{estimate_time_remaining(start_time, processed, total)}")

        df = get_stock_data(stock)
        if df is None:
            continue

        scores = calculate_scores(df)
        if scores is None:
            continue

        row = {"stock": stock}
        row.update(scores)
        master.append(row)

    master = pd.DataFrame(master)

    # ---- SAFE scoring ----
    master["score_short"] = master.apply(lambda r: float(scenario_score(r, "short")), axis=1)
    master["score_medium"] = master.apply(lambda r: float(scenario_score(r, "medium")), axis=1)
    master["score_long"] = master.apply(lambda r: float(scenario_score(r, "long")), axis=1)
    master["star_score"] = master.apply(lambda r: float(star_score(r)), axis=1)

    # ---- Export ----
    master.to_excel("Master.xlsx", index=False)
    master.nlargest(20, "score_short").to_excel("Top20_ShortTerm.xlsx", index=False)
    master.nlargest(20, "score_medium").to_excel("Top20_MediumTerm.xlsx", index=False)
    master.nlargest(20, "score_long").to_excel("Top20_LongTerm.xlsx", index=False)
    master.nlargest(5, "star_score").to_excel("Star_Top5.xlsx", index=False)

    return master

# ------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------
if __name__ == "__main__":
    STOCKS = pd.read_csv("stocks.csv")["Symbol"].tolist()
    run_engine(STOCKS)


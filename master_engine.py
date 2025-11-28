import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
import os
import time

# ------------------------------------------------------------
# % CHANGE FUNCTION
# ------------------------------------------------------------
def pct_change(df, days):
    if len(df) <= days:
        return np.nan
    end = df["Close"].iloc[-1]
    start = df["Close"].iloc[-days-1]
    if pd.isna(start) or pd.isna(end):
        return np.nan
    return ((end - start) / start) * 100

# ------------------------------------------------------------
# DOWNLOAD STOCK (simple + stable)
# ------------------------------------------------------------
def download_stock(sym):
    try:
        df = yf.download(sym, period="5y", interval="1d", progress=False)
        if df is not None and not df.empty:
            df.to_csv(f"raw_data/{sym}.csv")
            return sym, df
    except:
        pass
    return sym, None

# ------------------------------------------------------------
# PROCESS STOCK → CALCULATE ALL SCORES
# ------------------------------------------------------------
def process(sym, df):
    try:
        latest = float(df["Close"].iloc[-1])

        out = {
            "Symbol": sym,
            "Latest Price": latest,
            "change_1w": pct_change(df, 5),
            "change_1m": pct_change(df, 21),
            "change_3m": pct_change(df, 63),
            "change_6m": pct_change(df, 126),
            "change_1y": pct_change(df, 252),
            "change_2y": pct_change(df, 504),
            "change_3y": pct_change(df, 756),
            "change_4y": pct_change(df, 1008),
            "change_5y": pct_change(df, 1260),
        }

        # SHORT TERM SCORE (25%)
        out["short_term"] = np.nanmean([
            out["change_1w"],
            out["change_1m"],
            out["change_3m"]
        ])

        # MEDIUM TERM SCORE (30%)
        out["medium_term"] = np.nanmean([
            out["change_6m"],
            out["change_1y"]
        ])

        # LONG TERM SCORE (45%)
        out["long_term"] = np.nanmean([
            out["change_2y"],
            out["change_3y"],
            out["change_4y"],
            out["change_5y"]
        ])

        # COMBINED SCORE
        out["combined_score"] = (
            out["short_term"] * 0.25 +
            out["medium_term"] * 0.30 +
            out["long_term"] * 0.45
        )

        return out

    except Exception:
        return None

# ------------------------------------------------------------
# MAIN ENGINE (CLEAN + STABLE)
# ------------------------------------------------------------
def run_engine():

    os.makedirs("raw_data", exist_ok=True)
    os.makedirs("output", exist_ok=True)

    tickers = pd.read_excel("master_template.xlsx")["Symbol"].dropna().tolist()

    results = []
    for sym in tickers:
        sym, df = download_stock(sym)
        if df is not None:
            out = process(sym, df)
            if out:
                results.append(out)
        time.sleep(0.2)

    master = pd.DataFrame(results)
    master = master.replace([np.inf, -np.inf], np.nan).fillna(0)

    # RANK BASED ON COMBINED SCORE
    # Ensure combined_score exists (compute if missing)
    if "combined_score" not in master.columns:
    master["combined_score"] = (
        master["short_term"] * 0.25 +
        master["medium_term"] * 0.30 +
        master["long_term"] * 0.45
    )
    master["Rank"] = master["combined_score"].rank(
        ascending=False,
        method="dense"
    ).astype(int)

    # ------------------------------------------------------------
    # OUTPUT SECTION
    # ------------------------------------------------------------
    now = datetime.now().strftime("%Y%m%d_%H%M")
    outfile = f"output/master_output_{now}.xlsx"

    with pd.ExcelWriter(outfile, engine="openpyxl") as writer:

        master.to_excel(writer, "Master", index=False)

        master.sort_values("short_term", ascending=False).head(20) \
            .to_excel(writer, "Top20_ShortTerm", index=False)

        master.sort_values("medium_term", ascending=False).head(20) \
            .to_excel(writer, "Top20_MediumTerm", index=False)

        master.sort_values("long_term", ascending=False).head(20) \
            .to_excel(writer, "Top20_LongTerm", index=False)

        master.sort_values("combined_score", ascending=False).head(5) \
            .to_excel(writer, "Star_Top5", index=False)

    print(f"✔ Output created: {outfile}")

# ------------------------------------------------------------
# RUN ENGINE
# ------------------------------------------------------------
if __name__ == "__main__":
    run_engine()

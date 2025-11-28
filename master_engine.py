import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
import os
import requests
import json
import time
from openpyxl import load_workbook
from openpyxl.formatting.rule import ColorScaleRule
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# TELEGRAM ALERTS
# ============================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def telegram(msg):
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                params={"chat_id": TELEGRAM_CHAT_ID, "text": msg}
            )
        except:
            pass

telegram("📡 Stock Engine Started (GitHub Actions)")


# ============================================================
# % CHANGE FUNCTION
# ============================================================
def pct_change(df, days):
    if len(df) <= days:
        return np.nan
    end = df["Close"].iloc[-1]
    start = df["Close"].iloc[-days-1]
    if pd.isna(start) or pd.isna(end):
        return np.nan
    return ((end - start) / start) * 100


# ============================================================
# RAW DATA CACHE
# ============================================================
CACHE_FOLDER = "raw_data"
os.makedirs(CACHE_FOLDER, exist_ok=True)

def cache_path(sym):
    return os.path.join(CACHE_FOLDER, f"{sym}.csv")


def load_from_cache(sym):
    fp = cache_path(sym)
    if os.path.exists(fp):
        try:
            df = pd.read_csv(fp, index_col=0, parse_dates=True)
            return df
        except:
            return None
    return None


def save_to_cache(sym, df):
    df.to_csv(cache_path(sym))


# ============================================================
# DOWNLOAD ONE STOCK WITH RETRY + CACHE + FAIL REMOVAL
# ============================================================
def download_stock(sym):
    cached = load_from_cache(sym)
    if cached is not None:
        return sym, cached, "cache"

    fails = 0
    for attempt in range(3):
        try:
            df = yf.download(sym, period="5y", interval="1d", progress=False)
            if df is not None and not df.empty:
                save_to_cache(sym, df)
                return sym, df, "fresh"
        except:
            pass

        fails += 1
        time.sleep(1 + attempt)

        if fails == 2:
            return sym, None, "remove"   # auto-remove

    return sym, None, "fail"


# ============================================================
# PROCESS ONE STOCK
# ============================================================
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

        out["short_term"] = np.nanmean([
            out["change_1w"], out["change_1m"], out["change_3m"]
        ])

        out["medium_term"] = np.nanmean([
            out["change_6m"], out["change_1y"]
        ])

        out["long_term"] = np.nanmean([
            out["change_2y"], out["change_3y"],
            out["change_4y"], out["change_5y"]
        ])

        out["combined_score"] = (
            out["short_term"] * 0.25 +
            out["medium_term"] * 0.30 +
            out["long_term"] * 0.45
        )

        return out
    except:
        return None


# ============================================================
# PROGRESS BAR + ETA
# ============================================================
def print_progress(done, total, start_time):
    pct = (done / total) * 100
    elapsed = time.time() - start_time
    speed = done / elapsed if elapsed > 0 else 0
    remaining = (total - done) / speed if speed > 0 else 0

    bar = "█" * int(pct // 2) + "░" * (50 - int(pct // 2))
    print(
        f"\r[{bar}] {pct:5.1f}%  |  {done}/{total} "
        f"| ETA: {remaining:5.1f}s",
        end=""
    )


# ============================================================
# EXCEL COLORING
# ============================================================
def apply_color(ws, col_letter, row_count):
    cell_range = f"{col_letter}2:{col_letter}{row_count}"
    rule = ColorScaleRule(
        start_type="min", start_color="FF0000",
        mid_type="percentile", mid_value=50, mid_color="FFFF00",
        end_type="max", end_color="00B050"
    )
    ws.conditional_formatting.add(cell_range, rule)


# ============================================================
# MAIN ENGINE
# ============================================================
def run_engine():
    tickers = pd.read_excel("master_template.xlsx")["Symbol"].dropna().tolist()

    results = []
    removed = []
    start_time = time.time()

    telegram(f"⏳ Downloading {len(tickers)} stocks… (multi-threaded + ETA)")

    # MULTITHREADED + PROGRESS BAR
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(download_stock, sym): sym for sym in tickers}
        done = 0

        for future in as_completed(futures):
            sym = futures[future]
            sym, df, status = future.result()

            if status == "remove":
                removed.append(sym)
            elif df is not None:
                results.append((sym, df))

            done += 1
            print_progress(done, len(tickers), start_time)

    print("\n")
    telegram(f"📥 Completed: {len(results)}/{len(tickers)} stocks")

    if removed:
        telegram(f"⚠️ Removed (failed twice): {', '.join(removed[:10])}…")

    # PROCESS
    rows = []
    for sym, df in results:
        out = process(sym, df)
        if out:
            rows.append(out)

    master = pd.DataFrame(rows)
    master = master.replace([np.inf, -np.inf], np.nan).fillna(0)

    master["Rank"] = master["combined_score"].rank(
        ascending=False, method="dense"
    ).astype(int)

    # OUTPUT
    os.makedirs("output", exist_ok=True)
    now = datetime.now().strftime("%Y%m%d_%H%M")
    outfile = f"output/master_output_{now}.xlsx"

    with pd.ExcelWriter(outfile, engine="openpyxl") as writer:
        master.to_excel(writer, "Master", index=False)
        master.sort_values("short_term", ascending=False).head(20).to_excel(writer, "Top20_ShortTerm", index=False)
        master.sort_values("medium_term", ascending=False).head(20).to_excel(writer, "Top20_MediumTerm", index=False)
        master.sort_values("long_term", ascending=False).head(20).to_excel(writer, "Top20_LongTerm", index=False)
        master.sort_values("combined_score", ascending=False).head(5).to_excel(writer, "Star_Top5", index=False)

    wb = load_workbook(outfile)
    ws = wb["Master"]
    rows_count = len(master) + 1

    for col in ["C","D","E","F","G","H","I","J","K","L","M","N","O"]:
        apply_color(ws, col, rows_count)

    wb.save(outfile)

    telegram("✅ Stock Engine Completed Successfully")


# ============================================================
# RUN ENGINE
# ============================================================
try:
    run_engine()
except Exception as e:
    telegram(f"❌ Stock Engine Error: {e}")
    raise

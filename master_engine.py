import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
import os
import requests
from openpyxl import load_workbook
from openpyxl.formatting.rule import ColorScaleRule

# ============================================================
#                  TELEGRAM ALERTS
# ============================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def telegram(msg):
    """Send Telegram alert safely."""
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
#                  % CHANGE FUNCTION
# ============================================================
def pct_change(df, days):
    if len(df) <= days:
        return np.nan

    try:
        end = df["Close"].iloc[-1]

        # Fix: safe lookup
        start_index = max(0, len(df) - 1 - days)
        start = df["Close"].iloc[start_index]

        if pd.isna(start) or pd.isna(end):
            return np.nan

        return ((end - start) / start) * 100

    except Exception:
        return np.nan

# ============================================================
#                  PROCESS ONE STOCK
# ============================================================
def process(sym):
    try:
        df = yf.download(sym, period="5y", interval="1d", progress=False)

        if df is None or df.empty:
            return None

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

        # Short-term
        out["short_term"] = np.nanmean([
            out["change_1w"], out["change_1m"], out["change_3m"]
        ])

        # Medium-term
        out["medium_term"] = np.nanmean([
            out["change_6m"], out["change_1y"]
        ])

        # Long-term
        out["long_term"] = np.nanmean([
            out["change_2y"], out["change_3y"],
            out["change_4y"], out["change_5y"]
        ])

        # Weighted Combined Score
        out["combined_score"] = (
            out["short_term"] * 0.25 +
            out["medium_term"] * 0.30 +
            out["long_term"] * 0.45
        )

        return out

    except:
        return None


# ============================================================
#                  COLORING FUNCTION
# ============================================================
def apply_color(ws, col_letter, row_count):
    cell_range = f"{col_letter}2:{col_letter}{row_count}"

    rule = ColorScaleRule(
        start_type="min", start_color="FF0000",    # Red
        mid_type="percentile", mid_value=50, mid_color="FFFF00",  # Yellow
        end_type="max", end_color="00B050"         # Green
    )

    ws.conditional_formatting.add(cell_range, rule)


# ============================================================
#                    MAIN ENGINE
# ============================================================
def run_engine():

    tickers = pd.read_excel("master_template.xlsx")["Symbol"].dropna().tolist()
    rows = []

    for sym in tickers:
        r = process(sym)
        if r:
            rows.append(r)

    master = pd.DataFrame(rows)

    # 🔥 CRITICAL FIX:
    # Replace all NaN / inf before ranking/sorting (prevents GitHub failure)
    master = master.replace([np.inf, -np.inf], np.nan)
    master = master.fillna(0)

    # Rank
    master["Rank"] = master["combined_score"].rank(
        ascending=False, method="dense"
    ).astype(int)

    # Output folder
    os.makedirs("output", exist_ok=True)
    now = datetime.now().strftime("%Y%m%d_%H%M")
    outfile = f"output/master_output_{now}.xlsx"

    # Write Excel
    with pd.ExcelWriter(outfile, engine="openpyxl") as writer:
        master.to_excel(writer, sheet_name="Master", index=False)

        master.sort_values("short_term", ascending=False).head(20) \
            .to_excel(writer, sheet_name="Top20_ShortTerm", index=False)

        master.sort_values("medium_term", ascending=False).head(20) \
            .to_excel(writer, sheet_name="Top20_MediumTerm", index=False)

        master.sort_values("long_term", ascending=False).head(20) \
            .to_excel(writer, sheet_name="Top20_LongTerm", index=False)

        master.sort_values("combined_score", ascending=False).head(5) \
            .to_excel(writer, sheet_name="Star_Top5", index=False)

    # Coloring
    wb = load_workbook(outfile)
    ws = wb["Master"]
    rows_count = len(master) + 1

    for col in ["C","D","E","F","G","H","I","J","K","L","M","N","O"]:
        apply_color(ws, col, rows_count)

    wb.save(outfile)
    telegram("✅ Stock Engine Completed Successfully")

    print("Done. Excel saved:", outfile)


# ============================================================
#                    RUN
# ============================================================
try:
    run_engine()
except Exception as e:
    telegram(f"❌ Stock Engine Error: {e}")
    raise

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import zipfile
import os

# =====================================================
# --------------   SUPPORT FUNCTIONS   ----------------
# =====================================================

def safe_float(v, default=np.nan):
    try:
        return float(v)
    except:
        return default


def get_price(symbol, date):
    """Get nearest available close price to given date."""
    start = date - timedelta(days=5)
    end = date + timedelta(days=2)
    df = yf.download(symbol, start=start.strftime("%Y-%m-%d"),
                     end=end.strftime("%Y-%m-%d"), progress=False)
    if df.empty:
        return np.nan
    df = df["Close"]
    df.index = pd.to_datetime(df.index).date
    candidates = [d for d in df.index if d <= date]
    if not candidates:
        return np.nan
    return safe_float(df.loc[max(candidates)])


def get_last_13_fridays():
    """Return last 13 Fridays (oldest first)."""
    today = datetime.now().date()
    last_friday = today - timedelta(days=(today.weekday() - 4) % 7)
    fridays = [last_friday - timedelta(weeks=i) for i in range(13)]
    return list(reversed(fridays))


def compute_technical(df):
    """Compute RSI, MACD, EMA_signal from daily price series."""
    if df.empty:
        return np.nan, np.nan, np.nan

    close = df["Close"]

    # RSI
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(span=14).mean()
    loss = -delta.clip(upper=0).ewm(span=14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))

    # MACD
    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()

    return (
        safe_float(rsi.iloc[-1]),
        safe_float(macd.iloc[-1]),
        safe_float(signal.iloc[-1])
    )


# =====================================================
# -------------- FUNDAMENTAL METRICS ------------------
# =====================================================
def generate_fundamental(symbols):
    rows = []
    for sym in symbols:
        print("Fundamental:", sym)

        today = datetime.now().date()
        dates = {
            "Price_Latest": today,
            "Price_1D_Ago": today - timedelta(days=1),
            "Price_5D_Ago": today - timedelta(days=5),
            "Price_3M_Ago": today - timedelta(days=90),
            "Price_6M_Ago": today - timedelta(days=180),
            "Price_1Y_Ago": today - timedelta(days=365),
        }

        data = {}
        for name, d in dates.items():
            data[name] = get_price(sym, d)

        # Returns
        def ret(a, b):
            return ((a - b) / b) if (b not in [0, np.nan]) else np.nan

        row = {
            "Symbol": sym,
            **data,
            "Return_1D": ret(data["Price_Latest"], data["Price_1D_Ago"]),
            "Return_5D": ret(data["Price_Latest"], data["Price_5D_Ago"]),
            "Return_3M": ret(data["Price_Latest"], data["Price_3M_Ago"]),
            "Return_6M": ret(data["Price_Latest"], data["Price_6M_Ago"]),
            "Return_1Y": ret(data["Price_Latest"], data["Price_1Y_Ago"]),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_csv("fundamental_scores.csv", index=False)
    return df


# =====================================================
# -------------- TECHNICAL METRICS --------------------
# =====================================================
def generate_technical(symbols):
    rows = []
    for sym in symbols:
        print("Technical:", sym)

        end = datetime.now().date()
        start = end - timedelta(days=400)
        df = yf.download(sym, start=start.strftime("%Y-%m-%d"),
                         end=end.strftime("%Y-%m-%d"),
                         progress=False)

        if df.empty:
            rows.append({"Symbol": sym})
            continue

        rsi, macd, ema = compute_technical(df)

        rows.append({
            "Symbol": sym,
            "RSI_Latest": rsi,
            "MACD_Latest": macd,
            "EMA_signal_Latest": ema
        })

    df = pd.DataFrame(rows)
    df.to_csv("technical_scores.csv", index=False)
    return df


# =====================================================
# -------------- VOLATILITY METRICS -------------------
# =====================================================
def generate_volatility(symbols):
    fridays = get_last_13_fridays()
    rows = []

    for sym in symbols:
        print("Volatility:", sym)

        prices = []
        for f in fridays:
            prices.append(get_price(sym, f))

        clean = [p for p in prices if not pd.isna(p)]
        if len(clean) < 2:
            std_pct = atr = wow = trend = np.nan
        else:
            rets = [(clean[i] - clean[i-1]) / clean[i-1] for i in range(1, len(clean))]
            std_pct = np.std(rets) * 100
            atr = np.mean(np.abs(rets)) * 100
            wow = rets[-1] * 100
            diffs = [clean[i] - clean[i-1] for i in range(1, len(clean))]
            trend = (sum(diffs) / np.mean(clean)) * 100

        row = {"Symbol": sym}
        for i, f in enumerate(fridays, start=1):
            row[f"Week_{i}_{f.strftime('%Y-%m-%d')}"] = prices[i-1]

        row.update({
            "Weekly_StdDev_Pct": std_pct,
            "Weekly_ATR_Pct": atr,
            "Weekly_WoW_Pct": wow,
            "Weekly_Trend_13wPct": trend,
        })

        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_csv("weekly_volatility.csv", index=False)
    return df


# =====================================================
# -------------- MASTER SCORE MERGE -------------------
# =====================================================
def build_master(fund, tech, vol):
    df = fund.merge(tech, on="Symbol", how="left")
    df = df.merge(vol[[
        "Symbol",
        "Weekly_StdDev_Pct",
        "Weekly_ATR_Pct",
        "Weekly_WoW_Pct",
        "Weekly_Trend_13wPct"
    ]], on="Symbol", how="left")

    df.to_csv("master_scores.csv", index=False)
    return df


# =====================================================
# ---------------------- MAIN -------------------------
# =====================================================
def main():
    symbols = pd.read_csv("stocks.csv")["Symbol"].dropna().unique().tolist()

    fund = generate_fundamental(symbols)
    tech = generate_technical(symbols)
    vol = generate_volatility(symbols)

    master = build_master(fund, tech, vol)

    # Create ZIP
    with zipfile.ZipFile("stock_reports.zip", "w", zipfile.ZIP_DEFLATED) as z:
        z.write("fundamental_scores.csv")
        z.write("technical_scores.csv")
        z.write("weekly_volatility.csv")
        z.write("master_scores.csv")

    print("All reports generated and zipped: stock_reports.zip")


if __name__ == "__main__":
    main()

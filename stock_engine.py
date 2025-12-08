import yfinance as yf
import pandas as pd
import numpy as np


# ------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------

def get_price_n_bars_ago(close: pd.Series, n: int):
    """Return closing price n trading days ago (NaN if not enough history)."""
    if len(close) > n:
        return float(close.iloc[-(n + 1)])
    return np.nan


def compute_rsi(close: pd.Series, period: int = 14):
    """Classic RSI with EMA smoothing (Wilder-style). Returns full Series."""
    delta = close.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def safe_float(x):
    try:
        return float(x)
    except Exception:
        return np.nan


# ------------------------------------------------------------
# Load stock list from stocks.csv  (column: Symbol)
# ------------------------------------------------------------

stocks_df = pd.read_csv("stocks.csv")
symbols = stocks_df["Symbol"].dropna().astype(str).tolist()

# ------------------------------------------------------------
# Main loop – build rows for master sheet
# ------------------------------------------------------------

rows = []

# trading-day lookbacks (approximate)
LOOKBACKS = {
    "Close_1W": 5,
    "Close_1M": 21,
    "Close_3M": 63,
    "Close_6M": 126,
    "Close_1Y": 252,
    "Close_2Y": 504,
    "Close_3Y": 756,
    "Close_4Y": 1008,
    "Close_5Y": 1260,
}

for symbol in symbols:
    print(f"Processing: {symbol}")

    try:
        data = yf.download(
            symbol,
            period="10y",      # up to 10 years history
            interval="1d",
            progress=False,
            auto_adjust=False,
        )
    except Exception as e:
        print(f"  !!! Download failed for {symbol}: {e}")
        continue

    if data.empty or "Close" not in data.columns:
        print(f"  !!! No data for {symbol}")
        continue

    close = data["Close"].astype(float).dropna()

    if close.empty:
        print(f"  !!! Close series empty for {symbol}")
        continue

    # Latest close
    latest_close = safe_float(close.iloc[-1])

    # Lookback closes
    close_values = {}
    for col_name, n in LOOKBACKS.items():
        close_values[col_name] = get_price_n_bars_ago(close, n)

    # ---------------- Technicals ----------------
    # RSI
    rsi_series = compute_rsi(close, period=14)
    rsi_value = safe_float(rsi_series.iloc[-1])

    # MACD (12, 26, 9)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_series = ema12 - ema26
    signal_series = macd_series.ewm(span=9, adjust=False).mean()

    macd_value = safe_float(macd_series.iloc[-1])
    signal_value = safe_float(signal_series.iloc[-1])
    histogram_value = safe_float(macd_value - signal_value)

    # ---------------- Row for master sheet ----------------
    row = {
        "Symbol": symbol,
        "Latest Close": latest_close,
        "Close_1W": close_values["Close_1W"],
        "Close_1M": close_values["Close_1M"],
        "Close_3M": close_values["Close_3M"],
        "Close_6M": close_values["Close_6M"],
        "Close_1Y": close_values["Close_1Y"],
        "Close_2Y": close_values["Close_2Y"],
        "Close_3Y": close_values["Close_3Y"],
        "Close_4Y": close_values["Close_4Y"],
        "Close_5Y": close_values["Close_5Y"],
        "RSI": rsi_value,
        "MACD": macd_value,
        "Signal": signal_value,
        "Histogram": histogram_value,
    }

    rows.append(row)

# ------------------------------------------------------------
# Save master file (CSV)
# ------------------------------------------------------------

columns = [
    "Symbol",
    "Latest Close",
    "Close_1W",
    "Close_1M",
    "Close_3M",
    "Close_6M",
    "Close_1Y",
    "Close_2Y",
    "Close_3Y",
    "Close_4Y",
    "Close_5Y",
    "RSI",
    "MACD",
    "Signal",
    "Histogram",
]

df = pd.DataFrame(rows, columns=columns)
df.to_csv("master_sheet.csv", index=False)

print("master_sheet.csv created successfully.")

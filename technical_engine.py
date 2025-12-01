import pandas as pd
import yfinance as yf
import pandas_ta as ta
import time

MAX_RETRIES = 5
RETRY_DELAY = 2


def safe_yf_history(symbol, period):
    """Fetch data with retry logic to avoid yfinance rate-limits"""
    for attempt in range(MAX_RETRIES):
        try:
            data = yf.Ticker(symbol).history(period=period)
            if not data.empty:
                return data
        except Exception as e:
            print(f"[{symbol}] Data fetch attempt {attempt+1} failed: {e}")

        time.sleep(RETRY_DELAY)

    print(f"[{symbol}] FAILED to fetch price data after retries.")
    return pd.DataFrame()


def fallback_RSI(prices, period=14):
    """Manual RSI if pandas_ta fails"""
    delta = prices.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()

    rs = gain / loss
    return 100 - (100 / (1 + rs))


def fallback_EMA(prices, period=20):
    return prices.ewm(span=period, adjust=False).mean()


def fallback_MACD(prices):
    ema12 = prices.ewm(span=12, adjust=False).mean()
    ema26 = prices.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd, signal


def compute_scores(df):
    """Convert indicators into ranked percentile scores"""
    df["RSI_score"] = df["RSI"].rank(pct=True)
    df["MACD_score"] = df["MACD"].rank(pct=True)
    df["EMA_score"] = df["EMA20"].rank(pct=True)
    return df


def main():
    print("Running Technical Engine (Option A — stable)…")

    df = pd.read_csv("stocks.csv")
    stocks = df["Symbol"].tolist()

    out = []
    failed = []

    for i, symbol in enumerate(stocks):
        print(f"\n[{i+1}/{len(stocks)}] Processing: {symbol}")

        hist = safe_yf_history(symbol, "6mo")
        if hist.empty or len(hist) < 30:
            print(f"[{symbol}] Not enough data — removed.")
            failed.append(symbol)
            continue

        close = hist["Close"]

        # ---------------------------------------------
        # RSI (pandas_ta → fallback)
        # ---------------------------------------------
        try:
            rsi = ta.rsi(close, length=14).iloc[-1]
            if pd.isna(rsi):
                raise ValueError("RSI NA")
        except:
            rsi = fallback_RSI(close).iloc[-1]

        # ---------------------------------------------
        # MACD (pandas_ta → fallback)
        # ---------------------------------------------
        try:
            macd_df = ta.macd(close)
            macd = macd_df["MACD_12_26_9"].iloc[-1]
            signal = macd_df["MACDs_12_26_9"].iloc[-1]
            if pd.isna(macd):
                raise ValueError("MACD NA")
        except:
            macd, signal = fallback_MACD(close)
            macd = macd.iloc[-1]
            signal = signal.iloc[-1]

        # ---------------------------------------------
        # EMA20 (pandas_ta → fallback)
        # ---------------------------------------------
        try:
            ema20 = ta.ema(close, length=20).iloc[-1]
            if pd.isna(ema20):
                raise ValueError("EMA NA")
        except:
            ema20 = fallback_EMA(close).iloc[-1]

        out.append({
            "Symbol": symbol,
            "RSI": rsi,
            "MACD": macd,
            "EMA20": ema20
        })

    # ----------------------------------------------------
    # Save technical scores
    # ----------------------------------------------------
    df_t = pd.DataFrame(out)
    df_t = compute_scores(df_t)
    df_t.to_csv("technical_scores.csv", index=False)

    # Also log failed stocks
    if failed:
        pd.DataFrame({"Symbol": failed}).to_csv("technical_failed.csv", index=False)

    print("\nSaved: technical_scores.csv")
    if failed:
        print("Some stocks removed → see technical_failed.csv")


if __name__ == "__main__":
    main()

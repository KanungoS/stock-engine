import pandas as pd
import yfinance as yf
import time

MAX_RETRIES = 5
RETRY_DELAY = 2


def safe_yf_history(symbol, period="1y"):
    for attempt in range(MAX_RETRIES):
        try:
            data = yf.Ticker(symbol).history(period=period)
            if not data.empty:
                return data
        except Exception as e:
            print(f"[{symbol}] attempt {attempt+1} failed: {e}")
        time.sleep(RETRY_DELAY)
    print(f"[{symbol}] FAILED after all retries.")
    return pd.DataFrame()


def compute_fundamentals(df):
    df["score_short"] = df["5D_return"].rank(pct=True)
    df["score_medium"] = df["3M_return"].rank(pct=True)
    df["score_long"] = df["1Y_return"].rank(pct=True)
    return df


def main():
    print("Running Fundamental Engine…")

    df = pd.read_csv("stocks.csv")
    stocks = df["Symbol"].tolist()

    out = []
    failed = []

    for i, symbol in enumerate(stocks):
        print(f"\n[{i+1}/{len(stocks)}] {symbol}")

        hist = safe_yf_history(symbol)
        if hist.empty or len(hist) < 200:
            failed.append(symbol)
            continue

        price_now = hist["Close"].iloc[-1]
        price_5d = hist["Close"].iloc[-5] if len(hist) >= 5 else price_now
        price_3m = hist["Close"].iloc[-60] if len(hist) >= 60 else price_now
        price_1y = hist["Close"].iloc[0]

        out.append({
            "Symbol": symbol,
            "5D_return": (price_now - price_5d) / price_5d,
            "3M_return": (price_now - price_3m) / price_3m,
            "1Y_return": (price_now - price_1y) / price_1y,
        })

    df_f = pd.DataFrame(out)
    df_f = compute_fundamentals(df_f)
    df_f.to_csv("fundamental_scores.csv", index=False)

    if failed:
        pd.DataFrame({"Symbol": failed}).to_csv("fundamental_failed.csv", index=False)

    print("Saved: fundamental_scores.csv")


if __name__ == "__main__":
    main()

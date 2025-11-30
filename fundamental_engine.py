import pandas as pd
import yfinance as yf
import numpy as np

def safe_return(current, previous):
    if previous == 0 or pd.isna(previous):
        return 0
    return (current - previous) / previous

def compute_fundamental_score(df):
    df["score_short"] = df["5D_return"].rank(pct=True).fillna(0)
    df["score_medium"] = df["3M_return"].rank(pct=True).fillna(0)
    df["score_long"] = df["1Y_return"].rank(pct=True).fillna(0)
    return df

def main():
    print("Running Fundamental Engine...")

    df = pd.read_csv("stocks.csv")
    stocks = df["Symbol"].tolist()

    rows = []

    for symbol in stocks:
        print(f"Fetching: {symbol}")
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="1y")

            if hist.empty or len(hist) < 10:
                print(f"Insufficient data for {symbol}")
                continue

            close = hist["Close"]

            row = {
                "Symbol": symbol,
                "5D_return": safe_return(close.iloc[-1], close.iloc[-5] if len(close) > 5 else close.iloc[-1]),
                "3M_return": safe_return(close.iloc[-1], close.iloc[-60] if len(close) > 60 else close.iloc[0]),
                "1Y_return": safe_return(close.iloc[-1], close.iloc[0])
            }

            rows.append(row)

        except Exception as e:
            print(f"Error {symbol}: {e}")

    df_scores = pd.DataFrame(rows)
    df_scores = compute_fundamental_score(df_scores)
    df_scores.to_csv("fundamental_scores.csv", index=False)

    print("Saved: fundamental_scores.csv")

if __name__ == "__main__":
    main()

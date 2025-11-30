import pandas as pd
import yfinance as yf
import numpy as np
import talib

def main():
    print("Starting technical engine...")

    df = pd.read_csv("master_scores.csv")
    stocks = df["stock"].tolist()

    short_scores = []
    medium_scores = []
    long_scores = []

    for symbol in stocks:
        print(f"Processing: {symbol}")
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="1y")

            if len(hist) < 200:
                print(f"Insufficient data for {symbol}")
                short_scores.append(0)
                medium_scores.append(0)
                long_scores.append(0)
                continue

            close = hist["Close"].values.astype(float)

            rsi = talib.RSI(close, timeperiod=14)[-1]
            macd, signal, histo = talib.MACD(close)
            macd_last = macd[-1]
            sma20 = talib.SMA(close, timeperiod=20)[-1]
            sma50 = talib.SMA(close, timeperiod=50)[-1]
            sma200 = talib.SMA(close, timeperiod=200)[-1]
            price = close[-1]

            short = (rsi / 2) + (macd_last * 2)
            medium = (price - sma50) / sma50 * 100
            long = (price - sma200) / sma200 * 100

            short_scores.append(short)
            medium_scores.append(medium)
            long_scores.append(long)

        except Exception as e:
            print(f"Technical error for {symbol}: {e}")
            short_scores.append(0)
            medium_scores.append(0)
            long_scores.append(0)

    df["score_short"] = short_scores
    df["score_medium"] = medium_scores
    df["score_long"] = long_scores
    df["star_score"] = (
        df["score_short"] * 0.3 +
        df["score_medium"] * 0.3 +
        df["score_long"] * 0.4
    )

    df.to_csv("master_scores.csv", index=False)

    df.nlargest(20, "score_short").to_csv("Top20_Short.csv", index=False)
    df.nlargest(20, "score_medium").to_csv("Top20_Medium.csv", index=False)
    df.nlargest(20, "score_long").to_csv("Top20_Long.csv", index=False)
    df.nlargest(5, "star_score").to_csv("Star_Top5.csv", index=False)

    print("Technical engine completed.")

if __name__ == "__main__":
    main()

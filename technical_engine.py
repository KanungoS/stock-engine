import pandas as pd
import yfinance as yf
import talib
import numpy as np

def compute_technical_score(df):
    df["RSI_score"] = df["RSI"].rank(pct=True).fillna(0)
    df["MACD_score"] = df["MACD"].rank(pct=True).fillna(0)
    df["EMA_score"] = df["EMA_slope"].rank(pct=True).fillna(0)
    return df

def main():
    print("Running Technical Engine...")

    df = pd.read_csv("stocks.csv")
    stocks = df["Symbol"].tolist()

    rows = []

    for symbol in stocks:
        print(f"Processing: {symbol}")
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="6mo")

            if hist.empty or len(hist) < 30:
                print(f"Not enough chart data for {symbol}")
                continue

            close = hist["Close"]

            rsi = talib.RSI(close, timeperiod=14)
            macd, macd_signal, macd_hist = talib.MACD(close)
            ema20 = talib.EMA(close, timeperiod=20)

            row = {
                "Symbol": symbol,
                "RSI": rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 0,
                "MACD": macd.iloc[-1] if not pd.isna(macd.iloc[-1]) else 0,
                "EMA_slope": (ema20.iloc[-1] - ema20.iloc[-5]) if len(ema20) > 5 else 0
            }

            rows.append(row)

        except Exception as e:
            print(f"Error {symbol}: {e}")

    df_scores = pd.DataFrame(rows)
    df_scores = compute_technical_score(df_scores)
    df_scores.to_csv("technical_scores.csv", index=False)

    print("Saved: technical_scores.csv")

if __name__ == "__main__":
    main()

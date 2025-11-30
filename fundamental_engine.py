import pandas as pd
import yfinance as yf

def main():
    print("Starting fundamental engine...")

    df = pd.read_csv("stocks.csv")
    stocks = df["stock"].tolist()

    data = []

    for symbol in stocks:
        print(f"Fetching data: {symbol}")
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="5y")

            if hist.empty:
                print(f"No data found for {symbol}")
                continue

            row = {
                "stock": symbol,
                "price_today": hist["Close"].iloc[-1],
                "1w": hist["Close"].pct_change(5).iloc[-1] * 100,
                "1m": hist["Close"].pct_change(21).iloc[-1] * 100,
                "3m": hist["Close"].pct_change(63).iloc[-1] * 100,
                "6m": hist["Close"].pct_change(126).iloc[-1] * 100,
                "1y": hist["Close"].pct_change(252).iloc[-1] * 100,
                "2y": hist["Close"].pct_change(504).iloc[-1] * 100,
                "3y": hist["Close"].pct_change(756).iloc[-1] * 100,
                "4y": hist["Close"].pct_change(1008).iloc[-1] * 100,
                "5y": hist["Close"].pct_change(1260).iloc[-1] * 100,
            }

            data.append(row)

        except Exception as e:
            print(f"Error for {symbol}: {e}")

    df_out = pd.DataFrame(data)
    df_out.to_csv("master_scores.csv", index=False)

    print("Fundamental engine completed. Output: master_scores.csv")

if __name__ == "__main__":
    main()

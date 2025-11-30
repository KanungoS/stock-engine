import pandas as pd
import yfinance as yf

def compute_fundamental_score(df):
    """
    Computes short, medium, long-term scores from fundamental data.
    You may adjust scoring as needed.
    """

    # Example simple scoring logic — ADJUST BASED ON YOUR MODEL
    df["score_short"] = df["5D_return"].rank(pct=True)
    df["score_medium"] = df["3M_return"].rank(pct=True)
    df["score_long"] = df["1Y_return"].rank(pct=True)

    return df


def main():
    print("Running Fundamental Engine...")

    # Load stocks list
    df = pd.read_csv("stocks.csv")
    stocks = df["Symbol"].tolist()

    result_rows = []

    for symbol in stocks:
        print(f"Fetching data: {symbol}")
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="1y")

            if hist.empty:
                print(f"No data found for {symbol}")
                continue

            # Compute returns
            price_now = hist["Close"].iloc[-1]
            price_5d = hist["Close"].iloc[-5] if len(hist) > 5 else price_now
            price_3m = hist["Close"].iloc[-60] if len(hist) > 60 else price_now
            price_1y = hist["Close"].iloc[0]

            row = {
                "Symbol": symbol,
                "5D_return": (price_now - price_5d) / price_5d,
                "3M_return": (price_now - price_3m) / price_3m,
                "1Y_return": (price_now - price_1y) / price_1y
            }

            result_rows.append(row)

        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    # Create DataFrame
    df_scores = pd.DataFrame(result_rows)

    # Compute custom fundamental scores
    df_scores = compute_fundamental_score(df_scores)

    # Save output
    df_scores.to_csv("fundamental_scores.csv", index=False)
    print("Saved: fundamental_scores.csv")


if __name__ == "__main__":
    main()

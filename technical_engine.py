import pandas as pd
import yfinance as yf

def normalize_return(x):
    """Convert % return to score (0 – 100)."""
    if x is None:
        return 0
    if x > 30: return 100
    if x > 15: return 70
    if x > 5: return 50
    if x > 0: return 30
    return 10


def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss if loss.mean() != 0 else 0
    return 100 - (100 / (1 + rs))


def main():
    print("Running technical engine...")

    df = pd.read_csv("stocks.csv")
    stocks = df["Symbol"].tolist()

    final_rows = []

    for symbol in stocks:
        print(f"Fetching history: {symbol}")

        try:
            data = yf.Ticker(symbol).history(period="1y")

            if data.empty:
                print(f"No data for {symbol}")
                final_rows.append({
                    "Symbol": symbol,
                    "score_short": 0,
                    "score_medium": 0,
                    "score_long": 0
                })
                continue

            # Compute returns
            price_now = data["Close"].iloc[-1]
            price_1m = data["Close"].iloc[-22] if len(data) > 22 else price_now
            price_3m = data["Close"].iloc[-66] if len(data) > 66 else price_now
            price_6m = data["Close"].iloc[-132] if len(data) > 132 else price_now

            ret_1m = ((price_now - price_1m) / price_1m) * 100
            ret_3m = ((price_now - price_3m) / price_3m) * 100
            ret_6m = ((price_now - price_6m) / price_6m) * 100

            # Normalize
            score_short = normalize_return(ret_1m)
            score_medium = normalize_return(ret_3m)
            score_long = normalize_return(ret_6m)

            final_rows.append({
                "Symbol": symbol,
                "score_short": score_short,
                "score_medium": score_medium,
                "score_long": score_long
            })

        except Exception as e:
            print(f"Error for {symbol}: {e}")
            final_rows.append({
                "Symbol": symbol,
                "score_short": 0,
                "score_medium": 0,
                "score_long": 0
            })

    out = pd.DataFrame(final_rows)
    out.to_csv("technical_scores.csv", index=False)
    print("technical_scores.csv created.")


if __name__ == "__main__":
    main()

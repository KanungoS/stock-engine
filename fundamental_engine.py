import pandas as pd
import yfinance as yf

def score_fundamental(pe, pb, roe):
    """
    Creates a simple meaningful fundamental score.
    Lower P/E, lower P/B, higher ROE => better score.
    """
    score = 0

    # P/E scoring
    if pe and pe > 0:
        if pe < 15: score += 40
        elif pe < 25: score += 25
        else: score += 10

    # P/B scoring
    if pb and pb > 0:
        if pb < 2: score += 30
        elif pb < 4: score += 20
        else: score += 10

    # ROE scoring
    if roe:
        if roe > 25: score += 30
        elif roe > 15: score += 20
        else: score += 10

    return score


def main():
    print("Running fundamental engine...")

    df = pd.read_csv("stocks.csv")
    stocks = df["Symbol"].tolist()

    results = []

    for symbol in stocks:
        print(f"Fetching fundamentals: {symbol}")

        try:
            stk = yf.Ticker(symbol)
            info = stk.info

            pe = info.get("trailingPE")
            pb = info.get("priceToBook")
            roe = info.get("returnOnEquity")

            fundamental_score = score_fundamental(pe, pb, roe)

            results.append({
                "Symbol": symbol,
                "fundamental_score": fundamental_score
            })

        except Exception as e:
            print(f"Error for {symbol}: {e}")
            results.append({
                "Symbol": symbol,
                "fundamental_score": 0
            })

    # Save fundamental scores
    out = pd.DataFrame(results)
    out.to_csv("fundamental_scores.csv", index=False)
    print("fundamental_scores.csv created.")


if __name__ == "__main__":
    main()

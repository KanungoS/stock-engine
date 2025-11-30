import pandas as pd
import yfinance as yf
import talib

def compute_technical_score(df):
    """
    Compute technical indicators and convert them into ranks.
    Adjust scoring as per your needs.
    """

    df["RSI_score"] = df["RSI"].rank(pct=True)
    df["MACD_score"] = df["MACD"].rank(pct=True)
    df["EMA_score"] = df["EMA_signal"].rank(pct=True)

    return df


def main():
    print("Running Technical Engine...")

    # Load stocks
    df = pd.read_csv("stocks.csv")
    stocks = df["Symbol"].tolist()

    output = []

    for symbol in stocks:
        print(f"Fetching technicals for: {symbol}")

        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="6mo")

            if hist.empty:
                print(f"No chart data for {symbol}")
                continue

            close = hist["Close"]

            rsi = talib.RSI(close, timeperiod=14)
            macd, macd_signal, _ = talib.MACD(close)
            ema20 = talib.EMA(close, timeperiod=20)

            row = {
                "Symbol": symbol,
                "RSI": rsi.iloc[-1],
                "MACD": macd.iloc[-1],
                "EMA_signal": ema20.iloc[-1]
            }

            output.append(row)

        except Exception as e:
            print(f"Error: {symbol} - {e}")

    df_scores = pd.DataFrame(output)
    df_scores = compute_technical_score(df_scores)

    df_scores.to_csv("technical_scores.csv", index=False)
    print("Saved: technical_scores.csv")


if __name__ == "__main__":
    main()

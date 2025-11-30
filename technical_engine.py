import pandas as pd
import yfinance as yf
import pandas_ta as ta   # PURE PYTHON — NO TA-LIB NEEDED

def compute_technical_score(df):
    df["RSI_score"] = df["RSI"].rank(pct=True)
    df["MACD_score"] = df["MACD"].rank(pct=True)
    df["EMA_score"] = df["EMA_signal"].rank(pct=True)
    return df

def main():
    print("Running Technical Engine...")

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

            # --- Replace TA-Lib with pandas_ta ---
            rsi = ta.rsi(close, length=14).iloc[-1]
            macd_full = ta.macd(close)
            macd = macd_full["MACD_12_26_9"].iloc[-1]
            ema20 = ta.ema(close, length=20).iloc[-1]

            row = {
                "Symbol": symbol,
                "RSI": rsi,
                "MACD": macd,
                "EMA_signal": ema20
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

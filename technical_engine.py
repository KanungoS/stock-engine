import pandas as pd
import yfinance as yf
import pandas_ta as ta


def compute_technical_score(df):
    """
    Rank RSI, MACD, EMA20 into percentile scores.
    """
    df["RSI_score"] = df["RSI"].rank(pct=True)
    df["MACD_score"] = df["MACD"].rank(pct=True)
    df["EMA_score"] = df["EMA_signal"].rank(pct=True)

    return df


def main():
    print("Running Technical Engine...")

    # Load stocks list
    df = pd.read_csv("stocks.csv")
    stocks = df["Symbol"].tolist()

    technical_rows = []

    for symbol in stocks:
        print(f"Fetching technicals for: {symbol}")

        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="6mo")

            if hist.empty:
                print(f"No data for {symbol}")
                continue

            close = hist["Close"]

            # --- Indicators using pandas_ta ---
            rsi_series = ta.rsi(close, length=14)
            macd_df = ta.macd(close, fast=12, slow=26, signal=9)
            ema20_series = ta.ema(close, length=20)

            # Extract last values safely
            rsi_val = rsi_series.iloc[-1] if not rsi_series.empty else None
            ema_val = ema20_series.iloc[-1] if not ema20_series.empty else None

            if macd_df is not None and not macd_df.empty:
                macd_val = macd_df["MACD_12_26_9"].iloc[-1]
            else:
                macd_val = None

            technical_rows.append({
                "Symbol": symbol,
                "RSI": rsi_val,
                "MACD": macd_val,
                "EMA_signal": ema_val
            })

        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    # To DataFrame
    df_scores = pd.DataFrame(technical_rows)

    # Compute ranking-based technical score
    df_scores = compute_technical_score(df_scores)

    # Save output
    df_scores.to_csv("technical_scores.csv", index=False)
    print("Saved: technical_scores.csv")


if __name__ == "__main__":
    main()

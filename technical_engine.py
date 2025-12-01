import pandas as pd
import yfinance as yf

# ---------------------------
# MANUAL RSI
# ---------------------------
def compute_RSI(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# ---------------------------
# MANUAL MACD
# ---------------------------
def compute_MACD(series):
    ema12 = series.ewm(span=12, adjust=False).mean()
    ema26 = series.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd, signal

# ---------------------------
# MANUAL EMA-20
# ---------------------------
def compute_EMA(series, period=20):
    return series.ewm(span=period, adjust=False).mean()


def compute_technical_score(df):
    df["RSI_score"] = df["RSI"].rank(pct=True)
    df["MACD_score"] = df["MACD"].rank(pct=True)
    df["EMA_score"] = df["EMA_signal"].rank(pct=True)
    return df


def main():
    print("Running Technical Engine...")

    df = pd.read_csv("stocks.csv")
    symbols = df["Symbol"].tolist()

    results = []

    for symbol in symbols:
        print(f"Technical → {symbol}")
        try:
            hist = yf.Ticker(symbol).history(period="6mo")

            if hist.empty:
                print(f"No chart data for {symbol}")
                continue

            close = hist["Close"]

            rsi = compute_RSI(close)
            macd, macd_signal = compute_MACD(close)
            ema20 = compute_EMA(close)

            results.append({
                "Symbol": symbol,
                "RSI": rsi.iloc[-1],
                "MACD": macd.iloc[-1],
                "EMA_signal": ema20.iloc[-1]
            })

        except Exception as e:
            print(f"Error {symbol}: {e}")

    df_scores = pd.DataFrame(results)
    df_scores = compute_technical_score(df_scores)
    df_scores.to_csv("technical_scores.csv", index=False)
    print("Saved technical_scores.csv")


if __name__ == "__main__":
    main()

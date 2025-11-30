import pandas as pd
import yfinance as yf
import talib

# ------------------------------------------
# Load stock list
# ------------------------------------------
stocks = pd.read_csv("stocks.csv")["Symbol"].tolist()

# Output list
results = []

# ------------------------------------------
# Technical indicators to compute
# ------------------------------------------
def compute_indicators(close, high, low):
    indicators = {}

    # RSI
    indicators["RSI_14"] = float(talib.RSI(close, timeperiod=14)[-1])

    # MACD
    macd, macdsignal, macdhist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
    indicators["MACD"] = float(macd[-1])
    indicators["MACD_signal"] = float(macdsignal[-1])
    indicators["MACD_hist"] = float(macdhist[-1])

    # Moving averages
    indicators["SMA_20"] = float(talib.SMA(close, timeperiod=20)[-1])
    indicators["SMA_50"] = float(talib.SMA(close, timeperiod=50)[-1])
    indicators["SMA_200"] = float(talib.SMA(close, timeperiod=200)[-1])

    # Bollinger Bands
    upper, middle, lower = talib.BBANDS(close, timeperiod=20)
    indicators["BB_upper"] = float(upper[-1])
    indicators["BB_middle"] = float(middle[-1])
    indicators["BB_lower"] = float(lower[-1])

    return indicators

# ------------------------------------------
# Process each stock
# ------------------------------------------
for symbol in stocks:
    print(f"Processing TA-Lib indicators for: {symbol}")

    try:
        df = yf.download(symbol, period="1y", interval="1d", progress=False)

        if df.empty or len(df) < 60:
            continue

        close = df["Close"].astype(float)
        high = df["High"].astype(float)
        low = df["Low"].astype(float)

        indicators = compute_indicators(close, high, low)
        indicators["stock"] = symbol

        results.append(indicators)

    except Exception as e:
        print(f"Error with {symbol}: {e}")

# ------------------------------------------
# Save output
# ------------------------------------------
final_df = pd.DataFrame(results)
final_df.to_csv("technical_scores.csv", index=False)

print("technical_scores.csv created successfully.")

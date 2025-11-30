import yfinance as yf
import pandas as pd
import talib as ta

def compute_indicators(df):
    df["RSI"] = ta.RSI(df["Close"], timeperiod=14)
    df["SMA50"] = ta.SMA(df["Close"], timeperiod=50)
    df["SMA200"] = ta.SMA(df["Close"], timeperiod=200)
    df["MACD"], df["MACD_signal"], df["MACD_hist"] = ta.MACD(df["Close"])
    return df

def main():
    stocks = pd.read_csv("stocks.csv")["Symbol"].tolist()
    final = []

    for ticker in stocks:
        try:
            df = yf.download(ticker, period="1y", interval="1d", progress=False)
            if df.empty:
                continue

            df = compute_indicators(df)
            last = df.iloc[-1]

            final.append({
                "stock": ticker,
                "RSI": last["RSI"],
                "SMA50": last["SMA50"],
                "SMA200": last["SMA200"],
                "MACD": last["MACD"],
                "MACD_signal": last["MACD_signal"],
                "MACD_hist": last["MACD_hist"],
            })

        except Exception as e:
            print(f"Technical calc error for {ticker}: {e}")

    out = pd.DataFrame(final)
    out.to_csv("technical_scores.csv", index=False)
    print("technical_scores.csv created successfully")

if __name__ == "__main__":
    main()


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

            rsi = talib.RSI(close, timeperiod=14)[-1]
            macd, signal, histo = talib.MACD(close)
            macd_last = macd[-1]
            sma20 = talib.SMA(close, timeperiod=20)[-1]
            sma50 = talib.SMA(close, timeperiod=50)[-1]
            sma200 = talib.SMA(close, timeperiod=200)[-1]
            price = close[-1]

            short = (rsi / 2) + (macd_last * 2)
            medium = (price - sma50) / sma50 * 100
            long = (price - sma200) / sma200 * 100

            short_scores.append(short)
            medium_scores.append(medium)
            long_scores.append(long)

        except Exception as e:
            print(f"Technical error for {symbol}: {e}")
            short_scores.append(0)
            medium_scores.append(0)
            long_scores.append(0)

    df["score_short"] = short_scores
    df["score_medium"] = medium_scores
    df["score_long"] = long_scores
    df["star_score"] = (
        df["score_short"] * 0.3 +
        df["score_medium"] * 0.3 +
        df["score_long"] * 0.4
    )

    df.to_csv("master_scores.csv", index=False)

    df.nlargest(20, "score_short").to_csv("Top20_Short.csv", index=False)
    df.nlargest(20, "score_medium").to_csv("Top20_Medium.csv", index=False)
    df.nlargest(20, "score_long").to_csv("Top20_Long.csv", index=False)
    df.nlargest(5, "star_score").to_csv("Star_Top5.csv", index=False)

    print("Technical engine completed.")

if __name__ == "__main__":
    main()


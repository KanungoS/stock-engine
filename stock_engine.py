import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import numpy as np

# -------------------------------------------
# Helper Function: Fetch Historical Price
# -------------------------------------------
def get_price(hist, days_ago):
    """
    Returns the close price from N days ago.
    If insufficient data, returns NaN.
    """
    try:
        return hist["Close"].iloc[-days_ago]
    except:
        return np.nan


# -------------------------------------------
# Technical Indicator Calculations
# -------------------------------------------
def compute_RSI(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    RS = gain / loss
    RSI = 100 - (100 / (1 + RS))
    return RSI


def compute_MACD(series):
    ema12 = series.ewm(span=12, adjust=False).mean()
    ema26 = series.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd, signal


# -------------------------------------------
# MASTER ENGINE
# -------------------------------------------
def main():

    print("Loading stock list...")
    df_stocks = pd.read_csv("stocks.csv")
    symbols = df_stocks["Symbol"].tolist()

    fundamental_rows = []
    technical_rows = []
    master_rows = []

    print(f"Total stocks: {len(symbols)}")

    # Fetch 1 year of price data
    lookback_period = "400d"   # Ensures all windows exist (1Y, 6M, 3M, 5D, 1D)

    for symbol in symbols:
        print(f"Processing: {symbol}")

        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period=lookback_period)

            if hist.empty:
                print(f"No data for {symbol}")
                continue

            close = hist["Close"]

            # --- PRICE POINTS ---
            price_latest = get_price(hist, 1)
            price_1d = get_price(hist, 2)
            price_5d = get_price(hist, 6)
            price_3m = get_price(hist, 63)
            price_6m = get_price(hist, 126)
            price_1y = get_price(hist, 252)

            # --- RETURNS ---
            ret_1d = (price_latest - price_1d) / price_1d if price_1d else np.nan
            ret_5d = (price_latest - price_5d) / price_5d if price_5d else np.nan
            ret_3m = (price_latest - price_3m) / price_3m if price_3m else np.nan
            ret_6m = (price_latest - price_6m) / price_6m if price_6m else np.nan
            ret_1y = (price_latest - price_1y) / price_1y if price_1y else np.nan

            # --- TECHNICAL INDICATORS (Latest Window) ---
            rsi_series = compute_RSI(close)
            macd_series, signal_series = compute_MACD(close)

            # Helper to extract technicals correctly by shifted index
            def tech(series, days):
                try:
                    return series.iloc[-days]
                except:
                    return np.nan

            # --- LATEST TECHNICALS ---
            RSI_latest = tech(rsi_series, 1)
            MACD_latest = tech(macd_series, 1)
            Signal_latest = tech(signal_series, 1)

            # --- 1 DAY AGO ---
            RSI_1d = tech(rsi_series, 2)
            MACD_1d = tech(macd_series, 2)
            Signal_1d = tech(signal_series, 2)

            # --- 5 DAYS AGO ---
            RSI_5d = tech(rsi_series, 6)
            MACD_5d = tech(macd_series, 6)
            Signal_5d = tech(signal_series, 6)

            # --- 3 MONTHS AGO ---
            RSI_3m = tech(rsi_series, 63)
            MACD_3m = tech(macd_series, 63)
            Signal_3m = tech(signal_series, 63)

            # --- 6 MONTHS AGO ---
            RSI_6m = tech(rsi_series, 126)
            MACD_6m = tech(macd_series, 126)
            Signal_6m = tech(signal_series, 126)

            # --- 1 YEAR AGO ---
            RSI_1y = tech(rsi_series, 252)
            MACD_1y = tech(macd_series, 252)
            Signal_1y = tech(signal_series, 252)

            # -------------------------------------------------------
            # BUILD FUNDAMENTAL CSV ROW
            # -------------------------------------------------------
            fundamental_rows.append({
                "Symbol": symbol,
                "Price_Latest": price_latest,
                "Price_1D_Ago": price_1d,
                "Price_5D_Ago": price_5d,
                "Price_3M_Ago": price_3m,
                "Price_6M_Ago": price_6m,
                "Price_1Y_Ago": price_1y,
                "Return_1D": ret_1d,
                "Return_5D": ret_5d,
                "Return_3M": ret_3m,
                "Return_6M": ret_6m,
                "Return_1Y": ret_1y
            })

            # -------------------------------------------------------
            # BUILD TECHNICAL CSV ROW
            # -------------------------------------------------------
            technical_rows.append({
                "Symbol": symbol,
                "RSI_Latest": RSI_latest,
                "MACD_Latest": MACD_latest,
                "EMA_signal_Latest": Signal_latest,
                "RSI_1D_Ago": RSI_1d,
                "MACD_1D_Ago": MACD_1d,
                "EMA_signal_1D_Ago": Signal_1d,
                "RSI_5D_Ago": RSI_5d,
                "MACD_5D_Ago": MACD_5d,
                "EMA_signal_5D_Ago": Signal_5d,
                "RSI_3M_Ago": RSI_3m,
                "MACD_3M_Ago": MACD_3m,
                "EMA_signal_3M_Ago": Signal_3m,
                "RSI_6M_Ago": RSI_6m,
                "MACD_6M_Ago": MACD_6m,
                "EMA_signal_6M_Ago": Signal_6m,
                "RSI_1Y_Ago": RSI_1y,
                "MACD_1Y_Ago": MACD_1y,
                "EMA_signal_1Y_Ago": Signal_1y
            })

            # -------------------------------------------------------
            # MASTER CSV ROW (FUNDAMENTAL + TECHNICAL)
            # -------------------------------------------------------
            combined_row = {**fundamental_rows[-1], **technical_rows[-1]}
            master_rows.append(combined_row)

        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            continue

    # -------------------------------------------------------
    # SAVE ALL FILES
    # -------------------------------------------------------
    pd.DataFrame(fundamental_rows).to_csv("fundamental_scores.csv", index=False)
    pd.DataFrame(technical_rows).to_csv("technical_scores.csv", index=False)
    pd.DataFrame(master_rows).to_csv("master_scores.csv", index=False)

    print("All 3 files created successfully!")


if __name__ == "__main__":
    main()

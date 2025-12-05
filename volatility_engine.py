import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# -----------------------------------
# 1. Get last 13 Fridays
# -----------------------------------
def get_last_13_fridays():
    today = datetime.now().date()
    # Find most recent Friday
    last_friday = today - timedelta(days=(today.weekday() - 4) % 7)
    fridays = [last_friday - timedelta(weeks=i) for i in range(13)]
    return list(reversed(fridays))  # oldest → newest

def safe_float(v):
    try:
        if hasattr(v, "iloc"):
            return float(v.iloc[0])
        return float(v)
    except:
        return None

# -----------------------------------
# MAIN FUNCTION
# -----------------------------------
def main():
    stocks = pd.read_csv("stocks.csv")
    symbols = stocks["Symbol"].tolist()

    fridays = get_last_13_fridays()
    week_cols = [f"Week_{i+1}_{fridays[i]}" for i in range(13)]

    rows = []

    for symbol in symbols:
        print("Processing:", symbol)
        row = {"Symbol": symbol}

        ticker = yf.Ticker(symbol)

        # Pull DAILY data for weekly values
        hist_daily = ticker.history(period="120d", interval="1d")

        # Pull WEEKLY data for ATR
        hist_weekly = ticker.history(period="400d", interval="1wk")

        weekly_closes = []

        # -----------------------------------
        # 1. Find price on each Friday (or nearest earlier day)
        # -----------------------------------
        for i, fday in enumerate(fridays):
            available_days = hist_daily.index.date
            valid = max([d for d in available_days if d <= fday], default=None)

            col = f"Week_{i+1}_{fday}"

            if valid:
                close_raw = hist_daily.loc[hist_daily.index.date == valid]["Close"]
                close_val = safe_float(close_raw)
                row[col] = close_val
                weekly_closes.append(close_val)
            else:
                row[col] = None
                weekly_closes.append(None)

        # -----------------------------------
        # 2. Compute StdDev of 13 weekly values
        # -----------------------------------
        valid_closes = [c for c in weekly_closes if c is not None]

        if len(valid_closes) >= 2:
            row["Vol_StdDev_13W"] = float(pd.Series(valid_closes).std())
        else:
            row["Vol_StdDev_13W"] = None

        # -----------------------------------
        # 3. ATR using weekly OHLC data
        # -----------------------------------
        try:
            hist_weekly = hist_weekly.tail(13)
            high = hist_weekly["High"]
            low = hist_weekly["Low"]
            close = hist_weekly["Close"]

            prev_close = close.shift(1)

            TR = pd.concat([
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs()
            ], axis=1).max(axis=1)

            ATR = TR.mean()
            row["Vol_ATR_13W"] = float(ATR)
        except:
            row["Vol_ATR_13W"] = None

        # -----------------------------------
        # 4. %Δ WOW (Week-over-Week change)
        # -----------------------------------
        if weekly_closes[-1] and weekly_closes[-2]:
            row["Vol_Pct_WoW"] = (weekly_closes[-1] - weekly_closes[-2]) / weekly_closes[-2]
        else:
            row["Vol_Pct_WoW"] = None

        # -----------------------------------
        # 5. Custom Momentum Score
        # -----------------------------------
        try:
            diffs = []
            for i in range(12):
                if weekly_closes[i+1] is not None and weekly_closes[i] is not None:
                    diffs.append(weekly_closes[i+1] - weekly_closes[i])

            if len(diffs) >= 2 and len(valid_closes) > 0:
                momentum = sum(diffs) / (sum(valid_closes) / len(valid_closes))
                row["Vol_CustomMomentumScore"] = float(momentum)
            else:
                row["Vol_CustomMomentumScore"] = None
        except:
            row["Vol_CustomMomentumScore"] = None

        rows.append(row)

    # -----------------------------------
    # Create weekly_volatility.csv
    # -----------------------------------
    df = pd.DataFrame(rows)
    df.to_csv("weekly_volatility.csv", index=False)
    print("weekly_volatility.csv updated.")

    # -----------------------------------
    # Merge into master_scores.csv
    # -----------------------------------
    try:
        master = pd.read_csv("master_scores.csv")

        merge_cols = [
            "Vol_StdDev_13W",
            "Vol_ATR_13W",
            "Vol_Pct_WoW",
            "Vol_CustomMomentumScore",
        ]

        df_merge = df[["Symbol"] + merge_cols]

        master_updated = master.merge(df_merge, on="Symbol", how="left")
        master_updated.to_csv("master_scores.csv", index=False)

        print("master_scores.csv updated with volatility metrics.")
    except Exception as e:
        print("Could not merge into master_scores.csv:", e)


if __name__ == "__main__":
    main()

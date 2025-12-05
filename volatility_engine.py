import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

def get_last_13_fridays():
    """Return dates for the last 13 Fridays."""
    today = datetime.now()
    fridays = []

    # Find most recent Friday
    offset = (today.weekday() - 4) % 7
    last_friday = today - timedelta(days=offset)

    for i in range(13):
        fridays.append(last_friday - timedelta(weeks=i))

    return sorted([d.date() for d in fridays])

def fetch_weekly_close(symbol, dates):
    """Fetch the closing price nearest to each target Friday."""
    try:
        df = yf.download(symbol, period="100d", interval="1d", progress=False)
        df.index = df.index.date
    except:
        return [None] * len(dates)

    closes = []
    for d in dates:
        # If exact Friday exists, use it; else take nearest previous day
        available_dates = [x for x in df.index if x <= d]
        if available_dates:
            closes.append(df.loc[available_dates[-1], "Close"])
        else:
            closes.append(None)

    return closes

def main():
    print("Loading stock list...")
    stocks_df = pd.read_csv("stocks.csv")
    symbols = stocks_df["Symbol"].tolist()

    print("Computing last 13 Fridays...")
    fridays = get_last_13_fridays()

    output_rows = []
    print("Fetching 13-week weekly price history...")

    for sym in symbols:
        print(f"Processing: {sym}")
        closes = fetch_weekly_close(sym, fridays)
        row = {"Symbol": sym}
        for i, date in enumerate(fridays):
            row[f"Week_{i+1}_{date}"] = closes[i]
        output_rows.append(row)

    print("Saving weekly_volatility.csv ...")
    pd.DataFrame(output_rows).to_csv("weekly_volatility.csv", index=False)

    print("Weekly volatility file created successfully!")

if __name__ == "__main__":
    main()

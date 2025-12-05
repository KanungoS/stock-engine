import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def get_week_dates(n_weeks=13):
    today = datetime.now().date()
    dates = [(today - timedelta(weeks=i)) for i in range(1, n_weeks+1)]
    return sorted(dates)

def safe_float(x):
    """Convert Series/scalar to float safely."""
    try:
        if hasattr(x, "iloc"):   # If it's a Series
            return float(x.iloc[0])
        return float(x)
    except:
        return None

def main():
    # Load stock symbols
    stocks = pd.read_csv("stocks.csv")
    symbols = stocks["Symbol"].tolist()

    week_dates = get_week_dates()

    # Column names
    columns = ["Symbol"] + [
        f"Week_{i+1}_{week_dates[i]}" for i in range(len(week_dates))
    ]

    result = {col: [] for col in columns}

    for symbol in symbols:
        print("Processing:", symbol)

        result["Symbol"].append(symbol)

        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="120d")  # Fetch 120 days to cover 13+ weeks
        except:
            for col in columns[1:]:
                result[col].append(None)
            continue

        for i, d in enumerate(week_dates):
            col_name = f"Week_{i+1}_{d}"

            # If market closed on that date, use nearest previous date
            available_dates = hist.index.date
            valid_date = max([dt for dt in available_dates if dt <= d], default=None)

            if valid_date is None:
                result[col_name].append(None)
                continue

            price_raw = hist.loc[hist.index.date == valid_date]["Close"]

            # FIX: convert Series → float
            price = safe_float(price_raw)

            result[col_name].append(price)

    # Create final DataFrame
    df = pd.DataFrame(result)

    # Save CSV
    df.to_csv("weekly_volatility.csv", index=False)
    print("weekly_volatility.csv created successfully!")

if __name__ == "__main__":
    main()

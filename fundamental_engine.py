import pandas as pd
import yfinance as yf

# ------------------------------------------
# Load stock list
# ------------------------------------------
stocks = pd.read_csv("stocks.csv")["Symbol"].tolist()

# Time ranges (in days)
periods = {
    "1w": 5,
    "1m": 21,
    "3m": 63,
    "6m": 126,
    "1y": 252,
    "2y": 504,
    "3y": 756,
    "4y": 1008,
    "5y": 1260
}

# Empty list to collect results
results = []

# ------------------------------------------
# Process each stock
# ------------------------------------------
for symbol in stocks:
    print(f"Processing {symbol}...")

    try:
        data = yf.download(symbol, period="5y", interval="1d", progress=False)

        if data.empty:
            continue

        row = {"stock": symbol, "price_today": data["Close"].iloc[-1]}

        for label, days in periods.items():
            if len(data) > days:
                past_price = data["Close"].iloc[-days]
                ret = ((data["Close"].iloc[-1] - past_price) / past_price) * 100
            else:
                ret = 0
            row[label] = round(ret, 2)

        results.append(row)

    except:
        print(f"Error processing {symbol}")

# ------------------------------------------
# Convert to DataFrame
# ------------------------------------------
df = pd.DataFrame(results)

# ------------------------------------------
# Weightages (example structure)
# ------------------------------------------
df["score_short"] = df[["1w", "1m"]].mean(axis=1)
df["score_medium"] = df[["3m", "6m", "1y"]].mean(axis=1)
df["score_long"] = df[["2y", "3y", "4y", "5y"]].mean(axis=1)

df["star_score"] = (
    df["score_short"] * 0.3 +
    df["score_medium"] * 0.3 +
    df["score_long"] * 0.4
)

# ------------------------------------------
# Save final output
# ------------------------------------------
df.to_csv("master_scores.csv", index=False)

print("master_scores.csv created successfully.")

import pandas as pd
import numpy as np

# ------------------------------
# Load Input Files
# ------------------------------
fund = pd.read_csv("fundamental_scores.csv")   # from fundamental_engine.py
tech = pd.read_csv("technical_scores.csv")     # from technical_engine.py

# ------------------------------
# Merge Files
# ------------------------------
df = pd.merge(fund, tech, on="stock", how="inner")

# ------------------------------
# Weightages
# ------------------------------
WEIGHTS = {
    "short":  {"1w": 0.4, "1m": 0.4, "3m": 0.2, "RSI_14": -0.2, "MACD_hist": 0.2},
    "medium": {"6m": 0.3, "1y": 0.3, "2y": 0.2, "SMA_50": 0.1, "MACD": 0.1},
    "long":   {"3y": 0.3, "4y": 0.3, "5y": 0.2, "SMA_200": 0.2},
}

# ------------------------------
# Helper function for weighted scores
# ------------------------------
def compute_score(row, weight_dict):
    score = 0
    for col, w in weight_dict.items():
        if col in row:
            try:
                score += float(row[col]) * w
            except:
                score += 0
    return round(score, 2)

# ------------------------------
# Apply scoring
# ------------------------------
df["score_short"] = df.apply(lambda r: compute_score(r, WEIGHTS["short"]), axis=1)
df["score_medium"] = df.apply(lambda r: compute_score(r, WEIGHTS["medium"]), axis=1)
df["score_long"] = df.apply(lambda r: compute_score(r, WEIGHTS["long"]), axis=1)

# ------------------------------
# Composite STAR Score
# ------------------------------
df["star_score"] = (
    df["score_short"] * 0.3 +
    df["score_medium"] * 0.3 +
    df["score_long"] * 0.4
).round(2)

# ------------------------------
# Sort output
# ------------------------------
df_sorted = df.sort_values("star_score", ascending=False)

# ------------------------------
# Save full output
# ------------------------------
df_sorted.to_csv("combined_scores.csv", index=False)

# ------------------------------
# Save Top 20 Lists
# ------------------------------
df_sorted.nlargest(20, "score_short").to_csv("Top20_Short.csv", index=False)
df_sorted.nlargest(20, "score_medium").to_csv("Top20_Medium.csv", index=False)
df_sorted.nlargest(20, "score_long").to_csv("Top20_Long.csv", index=False)

# ------------------------------
# Save STAR Top 5
# ------------------------------
df_sorted.nlargest(5, "star_score").to_csv("Star_Top5.csv", index=False)

print("\nAll combined output files created successfully:")
print(" - combined_scores.csv")
print(" - Top20_Short.csv")
print(" - Top20_Medium.csv")
print(" - Top20_Long.csv")
print(" - Star_Top5.csv")

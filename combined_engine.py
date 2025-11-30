import pandas as pd

def main():
    print("Starting combined engine...")

    # Read fundamental + technical scores
    f1 = pd.read_csv("fundamental_scores.csv")
    t1 = pd.read_csv("technical_scores.csv")

    # Merge
    df = pd.merge(f1, t1, on="Symbol", how="inner")

    # Weighted final score
    df["combined_strength"] = (
        df["score_short"] * 0.25 +
        df["score_medium"] * 0.35 +
        df["score_long"] * 0.40
    )

    # Ranking
    df = df.sort_values(by="combined_strength", ascending=False)

    df.to_csv("master_scores.csv", index=False)
    print("master_scores.csv created successfully.")


if __name__ == "__main__":
    main()

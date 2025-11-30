import pandas as pd

def main():
    print("Running Combined Engine...")

    df_f = pd.read_csv("fundamental_scores.csv")
    df_t = pd.read_csv("technical_scores.csv")

    df = pd.merge(df_f, df_t, on="Symbol", how="inner")

    # Remove any rows with extreme NaN issues
    df = df.fillna(0)

    # Advanced combined weighting
    df["combined_strength"] = (
        df["score_short"] * 0.20 +
        df["score_medium"] * 0.30 +
        df["score_long"] * 0.30 +
        df["RSI_score"] * 0.05 +
        df["MACD_score"] * 0.10 +
        df["EMA_score"] * 0.05
    )

    df = df.sort_values(by="combined_strength", ascending=False)

    df.to_csv("master_scores.csv", index=False)
    print("Saved: master_scores.csv")

    df.nlargest(20, "score_short").to_csv("Top20_ShortTerm.csv", index=False)
    df.nlargest(20, "score_medium").to_csv("Top20_MediumTerm.csv", index=False)
    df.nlargest(20, "score_long").to_csv("Top20_LongTerm.csv", index=False)
    df.nlargest(5, "combined_strength").to_csv("Star_Top5.csv", index=False)
    df.nlargest(20, "combined_strength").to_csv("Top20_Combined.csv", index=False)

    print("All ranking files generated successfully!")

if __name__ == "__main__":
    main()

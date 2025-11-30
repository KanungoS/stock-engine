import pandas as pd

def main():
    print("Starting Combined Engine...")

    # Load both scores
    df_f = pd.read_csv("fundamental_scores.csv")
    df_t = pd.read_csv("technical_scores.csv")

    # Merge on Symbol
    df = pd.merge(df_f, df_t, on="Symbol", how="inner")

    # Compute final combined score (adjust weights as needed)
    df["combined_strength"] = (
        df["score_short"] * 0.25 +
        df["score_medium"] * 0.35 +
        df["score_long"] * 0.40
    )

    # Sort by combined score
    df = df.sort_values(by="combined_strength", ascending=False)

    # Save master output
    df.to_csv("master_scores.csv", index=False)
    print("Saved: master_scores.csv")

    # ================
    # TOP 20 LISTS
    # ================
    df.nlargest(20, "score_short").to_csv("Top20_ShortTerm.csv", index=False)
    df.nlargest(20, "score_medium").to_csv("Top20_MediumTerm.csv", index=False)
    df.nlargest(20, "score_long").to_csv("Top20_LongTerm.csv", index=False)

    # STAR STOCKS — Top 5 by combined
    df.nlargest(5, "combined_strength").to_csv("Star_Top5.csv", index=False)

    # Combined Top 20 final list
    df.nlargest(20, "combined_strength").to_csv("Top20_Combined.csv", index=False)

    print("All ranking files generated successfully!")


if __name__ == "__main__":
    main()

import pandas as pd

def main():
    print("Combining Engines…")

    df_f = pd.read_csv("fundamental_scores.csv")
    df_t = pd.read_csv("technical_scores.csv")

    df = pd.merge(df_f, df_t, on="Symbol", how="inner")

    # Combined strength (same weights as required)
    df["combined_strength"] = (
        df["score_short"] * 0.25 +
        df["score_medium"] * 0.35 +
        df["score_long"] * 0.40
    )

    df = df.sort_values(by="combined_strength", ascending=False)

    df.to_csv("master_scores.csv", index=False)

    df.nlargest(20, "score_short").to_csv("Top20_ShortTerm.csv", index=False)
    df.nlargest(20, "score_medium").to_csv("Top20_MediumTerm.csv", index=False)
    df.nlargest(20, "score_long").to_csv("Top20_LongTerm.csv", index=False)
    df.nlargest(5, "combined_strength").to_csv("Star_Top5.csv", index=False)
    df.nlargest(20, "combined_strength").to_csv("Top20_Combined.csv", index=False)

    print("All rankings generated.")


if __name__ == "__main__":
    main()

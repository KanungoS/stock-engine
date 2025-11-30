import pandas as pd

def main():
    print("Starting combined engine...")

    df = pd.read_csv("master_scores.csv")

    df["combined_strength"] = (
        df["score_short"] * 0.25 +
        df["score_medium"] * 0.35 +
        df["score_long"] * 0.40
    )

    df.to_csv("final_scores.csv", index=False)

    print("Combined engine completed. Output: final_scores.csv")

if __name__ == "__main__":
    main()

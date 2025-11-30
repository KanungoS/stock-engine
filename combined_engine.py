import pandas as pd
import subprocess
import os

# --------------------------------------------------
# 1. PATH SETUP
# --------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

fundamental_script = os.path.join(BASE_DIR, "fundamental_engine.py")
technical_script   = os.path.join(BASE_DIR, "technical_engine.py")

fundamental_output = os.path.join(BASE_DIR, "fundamental_scores.csv")
technical_output   = os.path.join(BASE_DIR, "technical_scores.csv")

final_master       = os.path.join(BASE_DIR, "final_master_scores.csv")

# --------------------------------------------------
# 2. Run fundamental_engine.py
# --------------------------------------------------
print("\nRunning FUNDAMENTAL ENGINE...")
subprocess.run(["python", fundamental_script], check=True)

# --------------------------------------------------
# 3. Run technical_engine.py
# --------------------------------------------------
print("\nRunning TECHNICAL ENGINE...")
subprocess.run(["python", technical_script], check=True)

# --------------------------------------------------
# 4. Load outputs
# --------------------------------------------------
print("\nLoading generated score files...")

fund_df = pd.read_csv(fundamental_output)
tech_df = pd.read_csv(technical_output)

# Ensure column consistency
fund_df.columns = fund_df.columns.str.lower()
tech_df.columns = tech_df.columns.str.lower()

# --------------------------------------------------
# 5. Merge both score sets
# --------------------------------------------------
print("\nMerging fundamental + technical scores...")

merged = pd.merge(fund_df, tech_df, on="stock", how="inner")

# --------------------------------------------------
# 6. Compute final COMBINED SCORES
# --------------------------------------------------
# You can adjust weights here anytime
WEIGHT_FUND = 0.60
WEIGHT_TECH = 0.40

merged["combined_score_short"]  = (merged["score_short_fund"]  * WEIGHT_FUND +
                                   merged["score_short_tech"]  * WEIGHT_TECH)

merged["combined_score_medium"] = (merged["score_medium_fund"] * WEIGHT_FUND +
                                   merged["score_medium_tech"] * WEIGHT_TECH)

merged["combined_score_long"]   = (merged["score_long_fund"]   * WEIGHT_FUND +
                                   merged["score_long_tech"]   * WEIGHT_TECH)

# Star Score = holistic long-term + medium-term blend
merged["star_score"] = (merged["combined_score_medium"] * 0.4 +
                        merged["combined_score_long"]   * 0.6)

# --------------------------------------------------
# 7. Save FINAL MASTER
# --------------------------------------------------
merged.to_csv(final_master, index=False)
print(f"\n✔ Saved MASTER SCORE → {final_master}")

# --------------------------------------------------
# 8. Generate TOP LISTS
# --------------------------------------------------
print("\nGenerating Top Lists...")

top20_short  = merged.sort_values("combined_score_short",  ascending=False).head(20)
top20_medium = merged.sort_values("combined_score_medium", ascending=False).head(20)
top20_long   = merged.sort_values("combined_score_long",   ascending=False).head(20)
star_top5    = merged.sort_values("star_score",            ascending=False).head(5)

top20_short.to_csv(os.path.join(BASE_DIR, "Top20_Short.csv"), index=False)
top20_medium.to_csv(os.path.join(BASE_DIR, "Top20_Medium.csv"), index=False)
top20_long.to_csv(os.path.join(BASE_DIR, "Top20_Long.csv"), index=False)
star_top5.to_csv(os.path.join(BASE_DIR, "Star_Top5.csv"), index=False)

print("\n✔ Top lists generated:")
print("   - Top20_Short.csv")
print("   - Top20_Medium.csv")
print("   - Top20_Long.csv")
print("   - Star_Top5.csv")

print("\nALL TASKS COMPLETE ✔")

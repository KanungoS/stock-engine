import subprocess
import sys

def run_script(script):
    print(f"\nRunning: {script}")
    result = subprocess.run([sys.executable, script], capture_output=True, text=True)

    if result.returncode != 0:
        print(f"❌ ERROR in {script}\n{result.stderr}")
        raise RuntimeError(f"{script} failed.")
    else:
        print(f"✔ Completed: {script}")

def main():
    # Order of execution
    run_script("fundamental_engine.py")     # Creates master_scores.csv
    run_script("technical_engine.py")       # Creates technical_scores.csv
    run_script("combined_engine.py")        # Creates all final outputs

    print("\n🎉 ALL ENGINES RAN SUCCESSFULLY")
    print("Outputs generated:")
    print(" - combined_scores.csv")
    print(" - Top20_Short.csv")
    print(" - Top20_Medium.csv")
    print(" - Top20_Long.csv")
    print(" - Star_Top5.csv\n")

if __name__ == "__main__":
    main()

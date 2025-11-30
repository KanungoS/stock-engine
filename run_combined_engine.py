import subprocess

def run_script(script):
    print(f"Running: {script}")
    result = subprocess.run(
        ["python", script],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise Exception(f"Script failed: {script}")

    print(result.stdout)


def main():
    print("=== Starting FULL Stock Engine Pipeline ===")

    # 1. Fundamental Engine → fundamental_scores.csv
    run_script("fundamental_engine.py")

    # 2. Technical Engine → technical_scores.csv
    run_script("technical_engine.py")

    # 3. Combined Engine → master_scores + rankings
    run_script("combined_engine.py")

    print("=== All engines completed successfully ===")


if __name__ == "__main__":
    main()

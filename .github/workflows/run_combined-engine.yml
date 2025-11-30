name: Run Combined Stock Engine

on:
  schedule:
    - cron: "5 3 * * *"   # Runs daily at 08:35 IST  (03:05 UTC)
  workflow_dispatch:     # Allow manual trigger

jobs:
  run-engine:
    runs-on: windows-latest

    steps:

    # -------------------------------------------------
    # 1. Checkout repo
    # -------------------------------------------------
    - name: Checkout repository
      uses: actions/checkout@v4

    # -------------------------------------------------
    # 2. Install Python
    # -------------------------------------------------
    - name: Set up Python 3.12
      uses: actions/setup-python@v5
      with:
        python-version: "3.12"

    # -------------------------------------------------
    # 3. Install dependencies
    # -------------------------------------------------
    - name: Install Python dependencies
      run: |
        python -m pip install --upgrade pip
        pip install yfinance pandas numpy ta-lib

    # -------------------------------------------------
    # 4. Run the combined engine
    # -------------------------------------------------
    - name: Run Combined Engine
      run: python combined_engine.py

    # -------------------------------------------------
    # 5. Commit output files back to repo
    # -------------------------------------------------
    - name: Commit results
      run: |
        git config --local user.email "actions@github.com"
        git config --local user.name "GitHub Actions"

        git add master_scores.csv || echo "master_scores.csv not found"
        git add Top20_Short.csv || echo "Top20_Short.csv not found"
        git add Top20_Medium.csv || echo "Top20_Medium.csv not found"
        git add Top20_Long.csv || echo "Top20_Long.csv not found"
        git add Star_Top5.csv || echo "Star_Top5.csv not found"

        git commit -m "Daily update: Combined Engine results" || echo "No changes to commit"

    # -------------------------------------------------
    # 6. Push results
    # -------------------------------------------------
    - name: Push results
      uses: ad-m/github-push-action@v0.8.0
      with:
        github_token: ${{ secrets.GITHUB_TOKEN }}

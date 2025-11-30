import subprocess

def run_script(script):
    print("Running script:", script)
    result = subprocess.run(["python", script], capture_output=True, text=True)

    if result.returncode != 0:
        print("Error while executing:", script)
        print(result.stderr)
        raise Exception("Script failed: " + script)
    else:
        print("Completed script:", script)

def main():
    scripts = [
        "fundamental_engine.py",
        "technical_engine.py",
        "combined_engine.py"
    ]

    for s in scripts:
        run_script(s)

if __name__ == "__main__":
    main()

"""
@file: main.py
@date: 23/02/2025
@author: roshin
"""

import os
import subprocess
import sys


def main():
    # Directory of the main.py file
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Path to the requirements.txt file
    requirements_path = os.path.join(script_dir, "requirements.txt")

    # Check if requirements.txt exists
    if os.path.isfile(requirements_path):
        # Install dependencies with warnings ignored
        subprocess.run(
            ["pip", "install", "-r", requirements_path],
            check=True,
            env={**os.environ, "PYTHONWARNINGS": "ignore"}
        )
    else:
        print("Error: 'requirements.txt' file not found.")
        sys.exit(1)  # Exit if requirements.txt is missing

    # Scripts to run
    scripts = ["NutritionTable.py"]

    # Check and run script
    for script in scripts:
        script_path = os.path.join(script_dir, script)
        if os.path.isfile(script_path):
            subprocess.run(
                ["python", script_path],
                check=True,
                env={**os.environ, "PYTHONWARNINGS": "ignore"}
            )
        else:
            print(f"Error: Script '{script}' not found.")
            sys.exit(1)  # Exit

    # Confirm
    print("Scripts executed successfully.")


if __name__ == "__main__":
    main()
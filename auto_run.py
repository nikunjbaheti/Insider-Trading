import subprocess
import os
from datetime import datetime
import glob

# Directory containing the scripts
scripts_dir = "/home/nikunj/NseInsiderTrading"

# List of Python scripts to run
scripts = [
    "Symbols.py",
    "StkData.py",
    "Updated_Mktcap.py",
    "promoter_pledge.py",
    "insider_trading_data.py",
    "commit_files.py"
]

# Log file
log_file = "/home/nikunj/NseInsiderTrading/log.txt"

# Function to delete .log and .txt files
def delete_log_and_text_files():
    # Find all .log and .txt files in the current directory
    for file_path in glob.glob("*.log") + glob.glob("*.txt"):
        try:
            os.remove(file_path)
            print(f"Deleted file: {file_path}")
        except Exception as e:
            print(f"Failed to delete {file_path}: {e}")

# Function to run each script and log output to file
def run_scripts():
    # Delete existing .log and .txt files before running the scripts
    delete_log_and_text_files()

    with open(log_file, "a") as log:  # Open in append mode to preserve logs if needed
        for script in scripts:
            try:
                # Log start time with timestamp
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log.write(f"[{timestamp}] Starting {script}...\n")
                print(f"[{timestamp}] Running {script}...")

                # Run the script with its full path
                result = subprocess.run(
                    ['python', os.path.join(scripts_dir, script)],
                    check=True,
                    capture_output=True,
                    text=True
                )

                # Log the output of the script with a timestamp
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log.write(f"[{timestamp}] Output of {script}:\n{result.stdout}\n")
                print(f"Output of {script} logged to {log_file}.")

            except subprocess.CalledProcessError as e:
                # Log the error with a timestamp
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log.write(f"[{timestamp}] Error occurred while running {script}:\n{e.stderr}\n")
                print(f"Error occurred while running {script}. Check {log_file} for details.")
                break  # Stop the process if an error occurs

if __name__ == "__main__":
    run_scripts()

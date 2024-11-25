import subprocess
import os
from datetime import datetime
import glob
import time

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
    for file_path in glob.glob("*.log") + glob.glob("*.txt"):
        try:
            os.remove(file_path)
            timestamped_message = f"[{current_timestamp()}] Deleted file: {file_path}\n"
            print(timestamped_message.strip())
        except Exception as e:
            timestamped_message = f"[{current_timestamp()}] Failed to delete {file_path}: {e}\n"
            print(timestamped_message.strip())

# Function to get the current timestamp
def current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Function to log a message to both console and log file
def log_message(log, message):
    log.write(message + "\n")
    print(message)

# Function to run each script and log output to file
def run_scripts():
    delete_log_and_text_files()

    with open(log_file, "a") as log:  # Open in append mode to preserve logs if needed
        for script in scripts:
            try:
                # Start message
                start_message = f"[{current_timestamp()}] Starting {script}..."
                log_message(log, start_message)

                # Measure time to start the script
                start_time = time.time()

                # Run the script
                process = subprocess.Popen(
                    ['python', os.path.join(scripts_dir, script)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                stdout, stderr = process.communicate()

                # Measure completion time
                end_time = time.time()
                elapsed_time = end_time - start_time
                timing_message = f"[{current_timestamp()}] {script} completed in {elapsed_time:.2f} seconds."
                log_message(log, timing_message)

                # Log and print stdout
                if stdout:
                    stdout_message = f"[{current_timestamp()}] Output of {script}:\n{stdout}"
                    log_message(log, stdout_message)

                # Log and print stderr
                if stderr:
                    stderr_message = f"[{current_timestamp()}] Errors in {script}:\n{stderr}"
                    log_message(log, stderr_message)

                # Handle non-zero exit code
                if process.returncode != 0:
                    raise subprocess.CalledProcessError(process.returncode, script, stderr=stderr)

            except subprocess.CalledProcessError as e:
                # Log and print error
                error_message = f"[{current_timestamp()}] Error occurred while running {script}:\n{e.stderr}"
                log_message(log, error_message)
                break  # Stop the process if an error occurs

if __name__ == "__main__":
    run_scripts()

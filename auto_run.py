import subprocess
import os
from datetime import datetime
import glob
import time
import sys

# Directory containing the scripts
scripts_dir = "/home/nikunj/NseInsiderTrading"

# List of Python scripts to run
scripts = [
    "Symbols.py",
    "Updated_Mktcap.py",
    "insider_trading_data.py",
    "promoter_pledge.py",
    "StkData.py",
    "commit_files.py"
]

# Log file
log_file = "/home/nikunj/NseInsiderTrading/log.txt"

# Function to delete .log and .txt files
def delete_log_and_text_files():
    for file_path in glob.glob("*.log") + glob.glob("*.txt"):
        try:
            os.remove(file_path)
            timestamped_message = f"[{current_timestamp()}] Deleted file: {file_path}"
            print(timestamped_message)
        except Exception as e:
            timestamped_message = f"[{current_timestamp()}] Failed to delete {file_path}: {e}"
            print(timestamped_message)

# Function to get the current timestamp
def current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Function to log a message to both console and log file
def log_message(log, message):
    log.write(message + "\n")
    log.flush()
    print(message)

# Function to run each script and show output in real-time
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

                # Run the script with real-time output
                script_path = os.path.join(scripts_dir, script)
                try:
                    process = subprocess.Popen(
                        ['python', script_path],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        bufsize=1,  # Line buffered
                        universal_newlines=True
                    )
                except (FileNotFoundError, PermissionError) as e:
                    error_message = f"[{current_timestamp()}] Failed to start {script}: {e}"
                    log_message(log, error_message)
                    continue

                # Print stdout in real-time
                while True:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break
                    if output:
                        timestamped_output = f"[{current_timestamp()}] {script} output: {output.strip()}"
                        log_message(log, timestamped_output)
                        sys.stdout.flush()

                # Capture any remaining stderr
                _, stderr = process.communicate()

                # Explicitly close stdout and stderr
                process.stdout.close()
                process.stderr.close()

                # Measure completion time
                end_time = time.time()
                elapsed_time = end_time - start_time
                timing_message = f"[{current_timestamp()}] {script} completed in {elapsed_time:.2f} seconds."
                log_message(log, timing_message)

                # Log and print any stderr
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
                continue  # Move to the next script even if an error occurs

if __name__ == "__main__":
    run_scripts()

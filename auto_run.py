import subprocess

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
log_file = "log.txt"

# Function to run each script and log output to file
def run_scripts():
    with open(log_file, "w") as log:
        for script in scripts:
            try:
                log.write(f"Running {script}...\n")
                print(f"Running {script}...")
                
                result = subprocess.run(
                    ['python', script], 
                    check=True, 
                    capture_output=True, 
                    text=True
                )
                
                # Log the output of the script
                log.write(f"Output of {script}:\n{result.stdout}\n")
                print(f"Output of {script} logged to {log_file}.")
                
            except subprocess.CalledProcessError as e:
                # Log the error
                log.write(f"Error occurred while running {script}:\n{e.stderr}\n")
                print(f"Error occurred while running {script}. Check {log_file} for details.")
                break  # Stop the process if an error occurs

if __name__ == "__main__":
    run_scripts()

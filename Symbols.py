import csv
import datetime
import requests
from io import StringIO
import os

def fetch_data():
    # Initialize a session to handle requests
    session = requests.Session()

    # Define headers to be used in the request
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.nseindia.com/",
    }

    # Get the current date in DDMMYYYY format
    current_date = datetime.datetime.now().strftime('%d%m%Y')

    # Construct the Bhavcopy URL for the current date
    url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{current_date}.csv"

    # Define the output directory
    output_directory = "/home/nikunj/NseInsiderTrading"
    os.makedirs(output_directory, exist_ok=True)  # Ensure the directory exists
    output_file = os.path.join(output_directory, "Symbols.csv")  # Full path for the output file

    try:
        # Step 1: Fetch CSV data for the current date using session
        response = session.get(url, headers=headers)
        response.raise_for_status()

        # Parse CSV data
        csv_data_array = list(csv.reader(StringIO(response.text)))

        # Check if CSV has content beyond headers
        if len(csv_data_array) > 1:
            symbols = [row[0] for row in csv_data_array[1:]]  # Assuming "SYMBOL" is in the first column

            # Save the symbols to a CSV file
            with open(output_file, "w", newline="") as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow(["SYMBOL"])  # Write header
                csv_writer.writerows([[symbol] for symbol in symbols])

            print(f"Symbols saved to {output_file} for {current_date}")
            return

    except requests.exceptions.HTTPError as e:
        print(f"Error fetching data for {current_date}: {str(e)}")

    # Retry for the past 10 days if current day's data is unavailable
    for i in range(1, 11):
        past_date = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%d%m%Y')
        past_url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{past_date}.csv"

        try:
            past_response = session.get(past_url, headers=headers)
            past_response.raise_for_status()

            # Parse CSV data for the past date
            past_csv_data_array = list(csv.reader(StringIO(past_response.text)))

            # Check if CSV has content beyond headers
            if len(past_csv_data_array) > 1:
                symbols = [row[0] for row in past_csv_data_array[1:]]  # Assuming "SYMBOL" is in the first column

                # Save the symbols to a CSV file
                with open(output_file, "w", newline="") as csvfile:
                    csv_writer = csv.writer(csvfile)
                    csv_writer.writerow(["SYMBOL"])  # Write header
                    csv_writer.writerows([[symbol] for symbol in symbols])

                print(f"Symbols saved to {output_file} for {past_date}")
                break  # Exit the loop if data is found for any past date

        except requests.exceptions.HTTPError as past_error:
            print(f"Error fetching data for {past_date}: {str(past_error)}")

# Call the function
fetch_data()

import csv
import datetime
import requests
from io import StringIO

def fetch_data():
    # Initialize a session to handle cookies
    session = requests.Session()
    
    # Define headers to be used in the request
    headers = {
        "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest"
    }
    
    # Step 1: Visit NSE homepage to set initial cookies
    try:
        session.get("https://www.nseindia.com", headers=headers)
        print("Cookies set by visiting NSE homepage.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to set cookies: {str(e)}")
        return  # Exit if unable to set cookies
    
    # Get the current date in DDMMYYYY format
    current_date = datetime.datetime.now().strftime('%d%m%Y')
    
    # Construct the Bhavcopy URL for the current date
    url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{current_date}.csv"

    try:
        # Step 2: Fetch CSV data for the current date using session
        response = session.get(url, headers=headers)
        response.raise_for_status()

        # Parse CSV data
        csv_data_array = list(csv.reader(StringIO(response.text)))

        # Check if CSV has content beyond headers
        if len(csv_data_array) > 1:
            # Extract data from the "SYMBOL" column
            symbols = [row[0] for row in csv_data_array[1:]]  # Assuming "SYMBOL" is in the first column

            # Save the symbols to a CSV file
            with open("Symbols.csv", "w", newline="") as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow(["SYMBOL"])  # Write header
                csv_writer.writerows([[symbol] for symbol in symbols])

            print("Symbols saved to Symbols.csv for", current_date)
            return  # Exit after successfully saving

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
                # Extract data from the "SYMBOL" column
                symbols = [row[0] for row in past_csv_data_array[1:]]  # Assuming "SYMBOL" is in the first column

                # Save the symbols to a CSV file
                with open("Symbols.csv", "w", newline="") as csvfile:
                    csv_writer = csv.writer(csvfile)
                    csv_writer.writerow(["SYMBOL"])  # Write header
                    csv_writer.writerows([[symbol] for symbol in symbols])

                print("Symbols saved to Symbols.csv for", past_date)
                break  # Exit the loop if data is found for any past date

        except requests.exceptions.HTTPError as past_error:
            print(f"Error fetching data for {past_date}: {str(past_error)}")

# Call the function
fetch_data()

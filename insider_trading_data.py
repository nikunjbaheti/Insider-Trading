import requests
import pandas as pd
import logging
import os
from datetime import datetime, timedelta
from fake_useragent import UserAgent
import time

# Initialize fake user agent generator
ua = UserAgent()

# Function to fetch data from the NSE API
def get_data():
    print("Establishing session with NSE website...")
    logger.info("Establishing session with NSE website...")

    # Define the start and end date
    start_date = (datetime.now().date() - timedelta(days=120)).strftime("%d-%m-%Y")
    end_date = datetime.now().date().strftime("%d-%m-%Y")

    # Construct the API request URL
    url = f"https://www.nseindia.com/api/corporates-pit?index=equities&from_date={start_date}&to_date={end_date}"

    session = requests.Session()

    headers = {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/',
    }

    try:
        # Step 1: Visit NSE home page
        print("Visiting nseindia.com...")
        home_response = session.get("https://www.nseindia.com", headers=headers, timeout=10)
        home_response.raise_for_status()

        # Step 2: Visit corporate filings insider trading page
        print("Visiting corporate filings insider trading page...")
        insider_response = session.get("https://www.nseindia.com/companies-listing/corporate-filings-insider-trading", headers=headers, timeout=10)
        insider_response.raise_for_status()

    except Exception as e:
        error_text = f"Error visiting NSE pages: {str(e)}"
        print(error_text)
        logger.error(error_text)
        return error_text

    # Update headers for API call
    headers_api = {
        'User-Agent': ua.random,
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-insider-trading',
    }

    try:
        print("Fetching insider trading data from API...")
        logger.info("Fetching insider trading data from API...")
        response = session.get(url, headers=headers_api, timeout=15)
        response.raise_for_status()
    except Exception as e:
        error_text = f"Error fetching data from API: {str(e)}"
        print(error_text)
        logger.error(error_text)
        return error_text

    if response.status_code == 200:
        print("Data fetched successfully.")
        logger.info("Data fetched successfully.")
        return response.json()
    else:
        error_text = f"Request failed with status code {response.status_code}"
        print(error_text)
        logger.error(error_text)
        return error_text

# Define the output directory and ensure it exists
output_directory = "/home/nikunj/NseInsiderTrading"
os.makedirs(output_directory, exist_ok=True)

# Initialize logging
log_file_path = os.path.join(output_directory, "log.txt")
logging.basicConfig(filename=log_file_path, format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger()

# Fields to track
columns = [
    'symbol', 'company', 'anex', 'acqName', 'date', 'pid', 'buyValue',
    'sellValue', 'buyQuantity', 'sellquantity', 'secType', 'secAcq',
    'tdpTransactionType', 'xbrl', 'personCategory', 'befAcqSharesNo',
    'befAcqSharesPer', 'secVal', 'securitiesTypePost', 'afterAcqSharesNo',
    'afterAcqSharesPer', 'acqfromDt', 'acqtoDt', 'intimDt', 'acqMode',
    'derivativeType', 'exchange', 'remarks'
]

insider_file_path = os.path.join(output_directory, "insider.csv")

# Step 1: Load the existing CSV or create a blank DataFrame if it doesn't exist
print("Loading existing insider.csv file...")
try:
    insider_df = pd.read_csv(insider_file_path)
    print("Loaded existing insider.csv file.")
    logger.info("Loaded existing insider.csv file.")
except FileNotFoundError:
    insider_df = pd.DataFrame(columns=columns)
    print("insider.csv not found, creating a new file.")
    logger.info("insider.csv not found, creating a new file.")

# Step 2: Fetch data from the API
data = get_data()

# Step 3: If there's an error in fetching data, log it and stop
if isinstance(data, str):
    print(f"Error fetching data: {data}")
    logger.error(f"Error fetching data: {data}")
else:
    # Step 4: Process the data
    print("Processing fetched data...")
    logger.info("Processing fetched data...")
    insider_data = data.get('data', [])

    # Create a DataFrame to store today's new data
    day_df = pd.DataFrame(columns=columns)

    for trade_data in insider_data:
        # Extract data for the new row
        row = [
            trade_data.get('symbol'),
            trade_data.get('company'),
            trade_data.get('anex'),
            trade_data.get('acqName'),
            trade_data.get('date'),
            trade_data.get('pid'),
            trade_data.get('buyValue'),
            trade_data.get('sellValue'),
            trade_data.get('buyQuantity'),
            trade_data.get('sellquantity'),
            trade_data.get('secType'),
            trade_data.get('secAcq'),
            trade_data.get('tdpTransactionType'),
            trade_data.get('xbrl'),
            trade_data.get('personCategory'),
            trade_data.get('befAcqSharesNo'),
            trade_data.get('befAcqSharesPer'),
            trade_data.get('secVal'),
            trade_data.get('securitiesTypePost'),
            trade_data.get('afterAcqSharesNo'),
            trade_data.get('afterAcqSharesPer'),
            trade_data.get('acqfromDt'),
            trade_data.get('acqtoDt'),
            trade_data.get('intimDt'),
            trade_data.get('acqMode'),
            trade_data.get('derivativeType'),
            trade_data.get('exchange'),
            trade_data.get('remarks')
        ]

        # Append the row if the data matches the columns
        if len(row) == len(columns):
            day_df.loc[len(day_df)] = row
        else:
            warning_msg = f"Skipping row due to mismatched length: {row}"
            print(warning_msg)
            logger.warning(warning_msg)

    # Step 5: Check for duplicates by merging with the existing insider_df
    combined_df = pd.concat([insider_df, day_df], ignore_index=True)

    # Step 6: Drop duplicates based on all columns
    combined_df.drop_duplicates(inplace=True)

    # Step 7: Log how many entries were added
    entries_added = len(combined_df) - len(insider_df)
    print(f"Entries added today: {entries_added}")
    logger.info(f"Entries added today: {entries_added}")

    # Step 8: Save the updated DataFrame to the CSV file
    combined_df.to_csv(insider_file_path, index=False)
    print(f"insider.csv updated successfully with {len(combined_df)} total entries.")
    logger.info(f"insider.csv updated successfully with {len(combined_df)} total entries.")

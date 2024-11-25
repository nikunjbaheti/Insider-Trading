import requests
import pandas as pd
import logging
import os
from datetime import datetime

# Function to fetch data from the API
def get_data():
    url = "https://www.nseindia.com/api/corporate-pledgedata?index=equities"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-pledged-data'
    }

    with requests.Session() as session:
        try:
            print("Establishing session with NSE website...")
            logger.info("Establishing session with NSE website...")
            session.get("https://www.nseindia.com", headers=headers)
            
            print("Fetching pledged data...")
            logger.info("Fetching pledged data...")
            response = session.get(url, headers=headers)
            
            if response.status_code == 200:
                print("Data fetched successfully.")
                logger.info("Data fetched successfully.")
                return response.json()
            else:
                error_text = f"Request failed with status code {response.status_code}"
                print(error_text)
                logger.error(error_text)
                return error_text
        except Exception as e:
            print(f"An error occurred while fetching data: {str(e)}")
            logger.error(f"An error occurred while fetching data: {str(e)}")
            return None

# Initialize logging
log_directory = "/home/nikunj/NseInsiderTrading"
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, "data_update.log")

logging.basicConfig(filename=log_file_path, format='%(asctime)s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Fields to track
columns = ['company_name', 'total_promoter_holding_pct', 'promoter_shares_encumbered', 'promoter_shares_encumbered_pct']

output_directory = "/home/nikunj/NseInsiderTrading"
os.makedirs(output_directory, exist_ok=True)
pledge_file_path = os.path.join(output_directory, "pledge.csv")

# Step 1: Load the existing CSV or create a blank DataFrame if it doesn't exist
try:
    print("Loading existing pledge.csv file...")
    pledge_df = pd.read_csv(pledge_file_path)
    print("Loaded existing pledge.csv file.")
    logger.info("Loaded existing pledge.csv file.")
except FileNotFoundError:
    print("pledge.csv not found. Creating a new file.")
    pledge_df = pd.DataFrame(columns=columns)
    logger.info("pledge.csv not found. Creating a new file.")

# Step 2: Fetch data from the API
data = get_data()

if isinstance(data, str) or data is None:
    print("Error fetching data. Exiting script.")
    logger.error(f"Error fetching data: {data}")
else:
    print("Processing fetched data...")
    logger.info("Processing fetched data...")

    pledge_data = data.get('data', [])
    day_df = pd.DataFrame(columns=columns)

    for trade_data in pledge_data:
        company_name = trade_data.get('comName', None)
        total_promoter_holding_pct = trade_data.get('percPromoterHolding', None)
        promoter_shares_encumbered = trade_data.get('totPromoterShares', None)
        promoter_shares_encumbered_pct = trade_data.get('percPromoterShares', None)

        row = [
            company_name, total_promoter_holding_pct, 
            promoter_shares_encumbered, promoter_shares_encumbered_pct
        ]
        day_df.loc[len(day_df)] = row

    combined_df = pd.concat([pledge_df, day_df], ignore_index=True)
    combined_df.drop_duplicates(subset=columns, keep='first', inplace=True)

    entries_added = len(combined_df) - len(pledge_df)
    print(f"Entries added today: {entries_added}")
    logger.info(f"Entries added today: {entries_added}")

    combined_df.to_csv(pledge_file_path, index=False)
    print(f"pledge.csv updated successfully with {len(combined_df)} total entries.")
    logger.info(f"pledge.csv updated successfully with {len(combined_df)} total entries.")

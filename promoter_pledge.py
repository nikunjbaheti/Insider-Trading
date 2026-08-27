import requests
import pandas as pd
import logging
import os
import time
from datetime import datetime
from fake_useragent import UserAgent

# Initialize fake user agent
ua = UserAgent()

# Initialize logging
log_directory = "/home/nikunj/NseInsiderTrading"
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, "log.txt")

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
    pledge_df = pd.read_csv(pledge_file_path, dtype=str, low_memory=False)
    print("Loaded existing pledge.csv file.")
    logger.info("Loaded existing pledge.csv file.")
except FileNotFoundError:
    print("pledge.csv not found. Creating a new file.")
    pledge_df = pd.DataFrame(columns=columns)
    logger.info("pledge.csv not found. Creating a new file.")
except Exception as e:
    print(f"Error reading pledge.csv: {str(e)}")
    logger.error(f"Error reading pledge.csv: {str(e)}")
    pledge_df = pd.DataFrame(columns=columns)

# Function to fetch data from the API
def get_data():
    base_url = "https://www.nseindia.com"
    pledge_page_url = "https://www.nseindia.com/companies-listing/corporate-filings-pledged-data"
    api_url = "https://www.nseindia.com/api/corporate-pledgedata?index=equities"

    user_agent = ua.random
    common_headers = {
        'User-Agent': user_agent,
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'application/json, text/plain, */*',
        'Connection': 'keep-alive',
    }

    with requests.Session() as session:
        try:
            # Step 1: Visit NSE homepage
            print("Navigating to NSE homepage...")
            logger.info("Navigating to NSE homepage...")
            session.headers.update(common_headers)
            home_resp = session.get(base_url, timeout=10)
            home_resp.raise_for_status()

            time.sleep(2)

            # Step 2: Visit pledge filings page
            print("Navigating to pledge filings page...")
            logger.info("Navigating to pledge filings page...")
            pledge_resp = session.get(pledge_page_url, timeout=10)
            pledge_resp.raise_for_status()

            time.sleep(2)

            # Step 3: Call API with full headers
            print("Fetching pledged data from API...")
            logger.info("Fetching pledged data from API...")

            api_headers = {
                'User-Agent': user_agent,
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': pledge_page_url,
                'Origin': base_url,
                'X-Requested-With': 'XMLHttpRequest',
                'Connection': 'keep-alive',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'cors',
            }

            response = session.get(api_url, headers=api_headers, timeout=10)
            response.raise_for_status()

            print("Data fetched successfully.")
            logger.info("Data fetched successfully.")
            return response.json()

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e} | Status code: {e.response.status_code}")
            print(f"HTTP error: {e}")
            return None
        except Exception as e:
            print(f"An error occurred while fetching data: {str(e)}")
            logger.error(f"An error occurred while fetching data: {str(e)}")
            return None

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

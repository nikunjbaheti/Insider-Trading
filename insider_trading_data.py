import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from fake_useragent import UserAgent

# Output directory setup
output_directory = "/home/nikunj/NseInsiderTrading"
os.makedirs(output_directory, exist_ok=True)

# Logging setup
log_file_path = os.path.join(output_directory, "log.txt")
logging.basicConfig(filename=log_file_path, format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger()

# UserAgent setup
ua = UserAgent()

def get_headers(user_agent, referer_url):
    return {
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer_url,
    }

def warm_up_session(session, user_agent):
    try:
        session.get("https://www.nseindia.com", headers=get_headers(user_agent, "https://www.nseindia.com"), timeout=10)
        time.sleep(1)
        session.get("https://www.nseindia.com/companies-listing/corporate-filings-insider-trading",
                    headers=get_headers(user_agent, "https://www.nseindia.com"), timeout=10)
        time.sleep(1)
    except Exception as e:
        logger.warning(f"Session warm-up failed: {str(e)}")
        print(f"Session warm-up failed: {str(e)}")

def get_data(session, user_agent):
    start_date = (datetime.now().date() - timedelta(days=120)).strftime("%d-%m-%Y")
    end_date = datetime.now().date().strftime("%d-%m-%Y")

    url = f"https://www.nseindia.com/api/corporates-pit?index=equities&from_date={start_date}&to_date={end_date}"
    headers = get_headers(user_agent, "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading")

    try:
        response = session.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        logger.info("Data fetched successfully from NSE Insider API")
        return response.json()
    except Exception as e:
        logger.error(f"API request failed: {str(e)}")
        print(f"API request failed: {str(e)}")
        return None

# File path
insider_file_path = os.path.join(output_directory, "insider.csv")

# Initialize session
session = requests.Session()
user_agent = ua.random
warm_up_session(session, user_agent)

# Fetch new data
data = get_data(session, user_agent)

# Define columns
columns = [
    'symbol', 'company', 'anex', 'acqName', 'date', 'pid', 'buyValue',
    'sellValue', 'buyQuantity', 'sellquantity', 'secType', 'secAcq',
    'tdpTransactionType', 'xbrl', 'personCategory', 'befAcqSharesNo',
    'befAcqSharesPer', 'secVal', 'securitiesTypePost', 'afterAcqSharesNo',
    'afterAcqSharesPer', 'acqfromDt', 'acqtoDt', 'intimDt', 'acqMode',
    'derivativeType', 'exchange', 'remarks'
]

if data and isinstance(data, dict):
    day_df = pd.DataFrame(columns=columns)
    for trade in data.get("data", []):
        row = [trade.get(col) for col in columns]
        if len(row) == len(columns):
            day_df.loc[len(day_df)] = row
        else:
            logger.warning(f"Skipped row due to mismatched length: {row}")

    # Write fresh data (overwrite the old file completely)
    day_df.to_csv(insider_file_path, index=False)
    print(f"✅ insider.csv created/replaced successfully with {len(day_df)} rows.")
    logger.info(f"insider.csv replaced successfully with {len(day_df)} rows.")
else:
    print("❌ No new data fetched or API failed.")
    logger.error("No data fetched or JSON structure invalid.")

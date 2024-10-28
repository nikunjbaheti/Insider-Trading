import requests
import pandas as pd
import logging
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
        # Access the main page to establish a session and get cookies
        session.get("https://www.nseindia.com", headers=headers)
        
        # Now, request the pledge data
        response = session.get(url, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            error_text = f"Request failed with status code {response.status_code}"
            return error_text

# Initialize logging
logging.basicConfig(filename="data_update.log", format='%(asctime)s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Fields to track
columns = ['company_name', 'total_promoter_holding_pct', 'promoter_shares_encumbered', 'promoter_shares_encumbered_pct']

pledge_file_path = 'pledge.csv'

# Step 1: Load the existing CSV or create a blank DataFrame if it doesn't exist
try:
    pledge_df = pd.read_csv(pledge_file_path)
    logger.info('Loaded existing pledge.csv file')
except FileNotFoundError:
    pledge_df = pd.DataFrame(columns=columns)
    logger.info('pledge.csv not found, creating a new file')

# Step 2: Fetch data from the API
data = get_data()

# Step 3: If there's an error in fetching data, log it and stop
if isinstance(data, str):
    logger.error(f'Error fetching data: {data}')
else:
    logger.info('Data successfully fetched from the API')

    # Step 4: Process the data
    pledge_data = data['data']
    
    # Create a DataFrame to store today's new data
    day_df = pd.DataFrame(columns=columns)
    
    for trade_data in pledge_data:
        company_name = trade_data.get('comName', None)
        total_promoter_holding_pct = trade_data.get('percPromoterHolding', None)
        promoter_shares_encumbered = trade_data.get('totPromoterShares', None)
        promoter_shares_encumbered_pct = trade_data.get('percPromoterShares', None)
        
        # Construct a new row
        row = [
            company_name, total_promoter_holding_pct, 
            promoter_shares_encumbered, promoter_shares_encumbered_pct
        ]

        # Add row to day_df (we'll check duplicates later)
        day_df.loc[len(day_df)] = row
    
    # Step 5: Check for duplicates by merging with the existing pledge_df
    combined_df = pd.concat([pledge_df, day_df], ignore_index=True)

    # Step 6: Drop duplicates based on the specified columns
    combined_df.drop_duplicates(subset=columns, keep='first', inplace=True)

    # Step 7: Log how many entries were added
    entries_added = len(combined_df) - len(pledge_df)
    logger.info(f'Entries added today: {entries_added}')
    
    # Step 8: Save the updated DataFrame to the CSV file
    combined_df.to_csv(pledge_file_path, index=False)
    logger.info(f'pledge.csv updated successfully with {len(combined_df)} total entries')


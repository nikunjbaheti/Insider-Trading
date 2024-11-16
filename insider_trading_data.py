import requests
import pandas as pd
import logging
import os
from datetime import datetime, timedelta

# Function to fetch data from the NSE API
def get_data():
    # Define the start and end date
    start_date = (datetime.now().date() - timedelta(days=120)).strftime("%d-%m-%Y")
    end_date = datetime.now().date().strftime("%d-%m-%Y")

    # Construct the URL for the API request
    url = f"https://www.nseindia.com/api/corporates-pit?index=equities&from_date={start_date}&to_date={end_date}"

    # Set up a session
    session = requests.Session()

    # Set headers to make the request successful
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/',
    }

    # Make an initial request to establish a session and get cookies
    session.get('https://www.nseindia.com/', headers=headers)

    # Make the GET request to fetch data
    response = session.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        return response.json()
    else:
        error_text = f"Request failed with status code {response.status_code}"
        return error_text

# Define the output directory and ensure it exists
output_directory = "/home/nikunj/NseInsiderTrading"
os.makedirs(output_directory, exist_ok=True)

# Initialize logging
log_file_path = os.path.join(output_directory, "data_update.log")
logging.basicConfig(filename=log_file_path, format='%(asctime)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger()

# Fields to track
columns = ['ticker', 'company_name', 'regulation', 'person_name', 'person_category',
           'type_of_security_prior', 'no_of_security_prior', 'pct_shareholding_prior',
           'type_of_security_acquired', 'no_of_securities_acquired', 'value_of_securities_acquired',
           'transaction_type', 'type_of_security_post', 'no_of_security_post', 'pct_post',
           'date_of_allotment_acquisition_from', 'date_of_allotment_acquisition_to',
           'date_of_intimation_to_company', 'mode_of_acquisition', 'derivative_type_security',
           'derivative_contract_specification', 'notional_value_buy', 'units_contract_lot_size_buy',
           'notional_value_sell', 'units_contract_lot_size_sell', 'exchange', 'remark',
           'broadcast_date_and_time', 'xbrl_link']

insider_file_path = os.path.join(output_directory, "insider.csv")

# Step 1: Load the existing CSV or create a blank DataFrame if it doesn't exist
try:
    insider_df = pd.read_csv(insider_file_path)
    logger.info('Loaded existing insider.csv file')
except FileNotFoundError:
    insider_df = pd.DataFrame(columns=columns)
    logger.info('insider.csv not found, creating a new file')

# Step 2: Fetch data from the API
data = get_data()

# Step 3: If there's an error in fetching data, log it and stop
if isinstance(data, str):
    logger.error(f'Error fetching data: {data}')
else:
    logger.info('Data successfully fetched from the API')

    # Step 4: Process the data
    insider_data = data.get('data', [])
    
    # Create a DataFrame to store today's new data
    day_df = pd.DataFrame(columns=columns)
    
    for trade_data in insider_data:
        company_name = trade_data.get('company', None)
        ticker = trade_data.get('symbol', None)
        person_name = trade_data.get('acqName', None)
        regulation = trade_data.get('anex', None)
        person_category = trade_data.get('personCategory', '-')
        no_of_security_prior = trade_data.get('befAcqSharesNo', 0)
        pct_shareholding_prior = trade_data.get('befAcqSharesPer', 0)
        no_of_securities_acquired = trade_data.get('secAcq', 0)
        value_of_securities_acquired = trade_data.get('secVal', 0)
        transaction_type = trade_data.get('tdpTransactionType', None)
        no_of_security_post = trade_data.get('afterAcqSharesNo', 0)
        pct_post = trade_data.get('afterAcqSharesPer', 0)
        type_of_security_prior = trade_data.get('secType', None)
        type_of_security_acquired = trade_data.get('secType', None)
        type_of_security_post = trade_data.get('securitiesTypePost', None)
        date_of_allotment_acquisition_from = trade_data.get('acqfromDt', None)
        date_of_allotment_acquisition_to = trade_data.get('acqtoDt', None)
        date_of_intimation_to_company = trade_data.get('intimDt', None)
        exchange = trade_data.get('exchange', None)
        mode_of_acquisition = trade_data.get('acqMode', None)
        derivative_type_security = trade_data.get('derivativeType', None)
        derivative_contract_specification = trade_data.get('derivativeType', None)
        notional_value_buy = trade_data.get('derivativeType', None)
        units_contract_lot_size_buy = trade_data.get('derivativeType', None)
        notional_value_sell = trade_data.get('derivativeType', None)
        units_contract_lot_size_sell = trade_data.get('derivativeType', None)
        broadcast_date_and_time = trade_data.get('date', None)
        remark = trade_data.get('derivativeType', None)
        xbrl_link = trade_data.get('xbrl', None)

        # Create a new row
        row = [
            ticker, company_name, regulation, person_name, person_category,
            type_of_security_prior, no_of_security_prior, pct_shareholding_prior,
            type_of_security_acquired, no_of_securities_acquired, value_of_securities_acquired,
            transaction_type, type_of_security_post, no_of_security_post, pct_post,
            date_of_allotment_acquisition_from, date_of_allotment_acquisition_to,
            date_of_intimation_to_company, mode_of_acquisition, derivative_type_security,
            derivative_contract_specification, notional_value_buy, units_contract_lot_size_buy,
            notional_value_sell, units_contract_lot_size_sell, exchange, remark,
            broadcast_date_and_time, xbrl_link
        ]

        # Add row to day_df
        day_df.loc[len(day_df)] = row

    # Step 5: Check for duplicates by merging with the existing insider_df
    combined_df = pd.concat([insider_df, day_df], ignore_index=True)

    # Step 6: Drop duplicates based on the specified columns
    combined_df.drop_duplicates(subset=columns, keep='first', inplace=True)

    # Step 7: Log how many entries were added
    entries_added = len(combined_df) - len(insider_df)
    logger.info(f'Entries added today: {entries_added}')
    
    # Step 8: Save the updated DataFrame to the CSV file
    combined_df.to_csv(insider_file_path, index=False)
    logger.info(f'insider.csv updated successfully with {len(combined_df)} total entries')

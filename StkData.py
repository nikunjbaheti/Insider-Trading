from requests_html import HTMLSession
from fake_useragent import UserAgent
import pandas as pd
import os
import time
import logging

# Configure logging
log_file = '/home/nikunj/NseInsiderTrading/log.txt'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_and_print(message, level="info"):
    """Logs and prints a message."""
    if level.lower() == "error":
        logging.error(message)
    elif level.lower() == "warning":
        logging.warning(message)
    else:
        logging.info(message)
    print(message)

def get_stock_data(symbol, session):
    try:
        log_and_print(f"Establishing session for symbol: {symbol}")

        # Step 1: Navigate to NSE homepage to establish session
        homepage_url = "https://www.nseindia.com"
        session.get(homepage_url)
        time.sleep(1)

        # Step 2: Navigate to the symbol-specific quote page
        quote_url = f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
        session.get(quote_url)
        time.sleep(1)

        # Step 3: Fetch stock data via API
        log_and_print(f"Fetching API data for symbol: {symbol}")
        api_url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
        api_response = session.get(api_url)

        if api_response.status_code == 200:
            data = api_response.json()
            industry_info = data.get('industryInfo', {})
            return (
                industry_info.get('basicIndustry'),
                industry_info.get('industry'),
                industry_info.get('macro'),
                industry_info.get('sector')
            )
        else:
            log_and_print(f"Failed to fetch data for symbol: {symbol}, Status: {api_response.status_code}", level="error")
            return None

    except Exception as e:
        log_and_print(f"Exception occurred for symbol: {symbol}: {str(e)}", level="error")
        return None

# Input and output file paths
input_file = '/home/nikunj/NseInsiderTrading/Symbols.csv'
output_directory = '/home/nikunj/NseInsiderTrading'
output_file = os.path.join(output_directory, "stkdata.csv")

log_and_print(f"Reading symbols from {input_file}")
symbols_df = pd.read_csv(input_file)

log_and_print(f"Creating output directory at {output_directory}")
os.makedirs(output_directory, exist_ok=True)

# Generate dynamic User-Agent
ua = UserAgent()
user_agent = ua.random

log_and_print(f"Creating session with dynamic User-Agent: {user_agent}")
with HTMLSession() as session:
    session.headers.update({
        'User-Agent': user_agent,
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/',
    })

    # Initialize DataFrame for results
    columns = ['symbol', 'basic_industry', 'industry', 'macro', 'sector']
    stkdata_df = pd.DataFrame(columns=columns)

    log_and_print("Starting to process symbols")
    for index, row in symbols_df.iterrows():
        symbol = str(row.iloc[0]).strip().upper()
        log_and_print(f"Processing symbol: {symbol}")
        industry_data = get_stock_data(symbol, session)
        if industry_data:
            new_row = pd.DataFrame([{
                'symbol': symbol,
                'basic_industry': industry_data[0],
                'industry': industry_data[1],
                'macro': industry_data[2],
                'sector': industry_data[3],
            }])
            stkdata_df = pd.concat([stkdata_df, new_row], ignore_index=True)
        else:
            log_and_print(f"No data found for symbol: {symbol}", level="warning")
        log_and_print(f"Waiting to avoid rate-limiting for symbol: {symbol}")
        time.sleep(2)

    # Save results
    log_and_print(f"Saving data to {output_file}")
    stkdata_df.to_csv(output_file, index=False)
    log_and_print(f"Data saved successfully to {output_file}")


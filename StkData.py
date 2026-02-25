from fake_useragent import UserAgent
from requests_html import HTMLSession
import pandas as pd
import os
import time
import logging

# Configure logging
log_file = '/home/nikunj/NseInsiderTrading/log.txt'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_and_print(message, level="info"):
    if level.lower() == "error":
        logging.error(message)
    elif level.lower() == "warning":
        logging.warning(message)
    else:
        logging.info(message)
    print(message)

def establish_session(session, symbol):
    try:
        log_and_print(f"Visiting NSE homepage to establish cookies for symbol: {symbol}")
        homepage_url = "https://www.nseindia.com"
        resp = session.get(homepage_url)
        if resp.status_code != 200:
            log_and_print(f"Failed to load NSE homepage, status: {resp.status_code}", level="error")
            return False

        script_url = f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
        log_and_print(f"Visiting equity script page for symbol: {symbol}")
        resp2 = session.get(script_url)
        if resp2.status_code != 200:
            log_and_print(f"Failed to load equity script page, status: {resp2.status_code}", level="error")
            return False

        return True
    except Exception as e:
        log_and_print(f"Exception during session establishment: {e}", level="error")
        return False

def get_stock_data(symbol, session):
    try:
        if not establish_session(session, symbol):
            log_and_print(f"Session establishment failed for symbol: {symbol}", level="error")
            return None

        api_url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
        log_and_print(f"Fetching API data for symbol: {symbol}")
        api_response = session.get(api_url)

        # Check content-type header to verify if JSON is returned
        content_type = api_response.headers.get('Content-Type', '')
        if 'application/json' not in content_type:
            log_and_print(f"API response for {symbol} is not JSON. Content-Type: {content_type}", level="error")
            log_and_print(f"Response content (truncated): {api_response.text[:500]}", level="error")
            return None

        if api_response.status_code == 200:
            data = api_response.json()
            industry_info = data.get('industryInfo', {})
            return (
                industry_info.get('basicIndustry'),
                industry_info.get('industry'),
                industry_info.get('macro'),
                industry_info.get('sector')
            )
        elif api_response.status_code == 429:
            log_and_print(f"Rate limited by NSE for symbol: {symbol}", level="error")
            return None
        else:
            log_and_print(f"API call failed for symbol: {symbol}, Status: {api_response.status_code}", level="error")
            return None
    except Exception as e:
        log_and_print(f"Exception occurred while fetching stock data for symbol: {symbol}: {str(e)}", level="error")
        return None

def main():
    input_file = '/home/nikunj/NseInsiderTrading/Symbols.csv'
    output_directory = '/home/nikunj/NseInsiderTrading'
    output_file = os.path.join(output_directory, "stkdata.csv")

    log_and_print(f"Reading symbols from {input_file}")
    symbols_df = pd.read_csv(input_file)

    log_and_print(f"Creating output directory at {output_directory}")
    os.makedirs(output_directory, exist_ok=True)

    ua = UserAgent()

    with HTMLSession() as session:
        user_agent = ua.random
        log_and_print(f"Using User-Agent: {user_agent}")

        # Set headers that do not depend on symbol
        session.headers.update({
            'User-Agent': user_agent,
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'application/json, text/plain, */*',
        })

        columns = ['symbol', 'basic_industry', 'industry', 'macro', 'sector']
        stkdata_df = pd.DataFrame(columns=columns)

        log_and_print("Starting to process symbols")
        for index, row in symbols_df.iterrows():
            symbol = row.iloc[0]
            
            # Update Referer header dynamically per symbol
            session.headers.update({
                'Referer': f'https://www.nseindia.com/get-quotes/equity?symbol={symbol}',
            })

            log_and_print(f"Processing symbol: {symbol}")
            industry_data = get_stock_data(symbol, session)
            if industry_data:
                log_and_print(f"Data retrieved for symbol: {symbol}")
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

            log_and_print(f"Waiting 2 seconds to avoid rate-limiting for symbol: {symbol}")
            time.sleep(2)

        log_and_print(f"Saving data to {output_file}")
        stkdata_df.to_csv(output_file, index=False)
        log_and_print(f"Data saved successfully to {output_file}")

if __name__ == "__main__":
    main()

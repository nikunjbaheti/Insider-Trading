from requests_html import HTMLSession
import pandas as pd
import os
import time
import logging

logging.basicConfig(filename='/home/nikunj/NseInsiderTrading/error.log', level=logging.ERROR)

def get_stock_data(symbol, session):
    try:
        # Step 1: Navigate to NSE homepage to establish session
        homepage_url = "https://www.nseindia.com"
        session.get(homepage_url)

        # Step 2: Fetch stock data
        api_url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
        api_response = session.get(api_url)

        # Parse JSON data if the response is successful
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
            logging.error(f"Failed for {symbol} with status {api_response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Exception for {symbol}: {str(e)}")
        return None

# Input and output file paths
input_file = '/home/nikunj/NseInsiderTrading/Symbols.csv'
output_directory = '/home/nikunj/NseInsiderTrading'
output_file = os.path.join(output_directory, "stkdata.csv")

# Read symbols and create output directory
symbols_df = pd.read_csv(input_file)
os.makedirs(output_directory, exist_ok=True)

# Create session and set headers
with HTMLSession() as session:
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
    })

    # Initialize DataFrame for results
    columns = ['symbol', 'basic_industry', 'industry', 'macro', 'sector']
    stkdata_df = pd.DataFrame(columns=columns)

    for index, row in symbols_df.iterrows():
        symbol = row.iloc[0]
        print(f"Processing symbol: {symbol}")  # Added print statement to display current symbol
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
            logging.info(f"No data for {symbol}")
        time.sleep(2)  # Avoid rate-limiting

    # Save results
    stkdata_df.to_csv(output_file, index=False)
    logging.info(f"Data saved to {output_file}")

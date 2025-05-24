import os
import time
import logging
import pandas as pd
from requests_html import HTMLSession

# Configure logging
logging.basicConfig(
    filename='/home/nikunj/NseInsiderTrading/log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def initialize_session():
    """Initialize the session and set cookies by visiting NSE homepage."""
    session = HTMLSession()
    try:
        homepage_url = "https://www.nseindia.com"
        response = session.get(homepage_url)
        if response.status_code == 200:
            logging.info("Successfully initialized session with NSE website.")
        else:
            logging.warning(f"Failed to initialize session with status code {response.status_code}")
    except Exception as e:
        logging.error(f"Error initializing session: {str(e)}")
        session.close()
        raise
    return session

def get_market_cap(symbol):
    """Fetch market cap for a given symbol using a new session."""
    session = initialize_session()  # Create a new session for each symbol
    try:
        api_url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}&section=trade_info"
        api_response = session.get(api_url)

        if api_response.status_code == 200:
            data = api_response.json()
            total_market_cap = data.get('marketDeptOrderBook', {}).get('tradeInfo', {}).get('totalMarketCap', None)
            return total_market_cap
        else:
            logging.error(f"Request for symbol {symbol} failed with status code {api_response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Error for {symbol}: {str(e)}")
        return None
    finally:
        session.close()  # Close the session after each request

# Paths
symbols_file = '/home/nikunj/NseInsiderTrading/Symbols.csv'
output_file = '/home/nikunj/NseInsiderTrading/mktcap.csv'

# Validate input file
if not os.path.exists(symbols_file):
    raise FileNotFoundError(f"{symbols_file} not found!")

# Read symbols
symbols_df = pd.read_csv(symbols_file)
if symbols_df.empty or symbols_df.shape[1] < 1:
    raise ValueError(f"{symbols_file} is empty or incorrectly formatted!")

# Create a DataFrame for results
columns = ['symbol', 'market_cap']
mktcap_df = pd.DataFrame(columns=columns)

# Fetch market caps
for index, row in symbols_df.iterrows():
    symbol = row.iloc[0]
    market_cap = get_market_cap(symbol)

    if market_cap is not None:
        print(f"Market cap for {symbol}: {market_cap}")
        mktcap_df = mktcap_df._append({'symbol': symbol, 'market_cap': market_cap}, ignore_index=True)
    else:
        print(f"Error fetching market cap for {symbol}")

    time.sleep(1)  # Delay between requests

# Save results
os.makedirs(os.path.dirname(output_file), exist_ok=True)
mktcap_df.to_csv(output_file, index=False)
print(f"Market cap data saved to {output_file}")

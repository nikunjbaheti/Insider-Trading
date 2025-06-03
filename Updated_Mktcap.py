import os
import time
import logging
import pandas as pd
import requests
from fake_useragent import UserAgent

# Configure logging
logging.basicConfig(
    filename='/home/nikunj/NseInsiderTrading/log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize fake user-agent
ua = UserAgent()

def get_headers(user_agent):
    """Return headers with a given User-Agent."""
    return {
        "User-Agent": user_agent,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "*/*",
        "Connection": "keep-alive",
        "Referer": "https://www.nseindia.com/"
    }

def initialize_session(user_agent):
    """Initialize a session with NSE homepage and set cookies."""
    session = requests.Session()
    try:
        homepage = "https://www.nseindia.com"
        response = session.get(homepage, headers=get_headers(user_agent), timeout=10)
        if response.status_code == 200:
            logging.info("Session initialized successfully.")
        else:
            msg = f"Homepage visit returned {response.status_code}"
            logging.warning(msg)
            print(msg)
    except Exception as e:
        error_msg = f"Error initializing session: {str(e)}"
        logging.error(error_msg)
        print(error_msg)
        session.close()
        raise
    return session

def get_market_cap(symbol, session, user_agent):
    """Get market cap for the given symbol using provided session."""
    try:
        url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}&section=trade_info"
        response = session.get(url, headers=get_headers(user_agent), timeout=10)

        if response.status_code == 200:
            data = response.json()
            market_cap = data.get("marketDeptOrderBook", {}).get("tradeInfo", {}).get("totalMarketCap", None)
            return market_cap
        else:
            msg = f"{symbol} failed with status code {response.status_code}"
            logging.error(msg)
            print(msg)
            return None
    except Exception as e:
        error_msg = f"Error fetching {symbol}: {str(e)}"
        logging.error(error_msg)
        print(error_msg)
        return None

# File paths
symbols_file = '/home/nikunj/NseInsiderTrading/Symbols.csv'
output_file = '/home/nikunj/NseInsiderTrading/mktcap.csv'

# Validate input file
if not os.path.exists(symbols_file):
    raise FileNotFoundError(f"{symbols_file} not found!")

# Read symbols
symbols_df = pd.read_csv(symbols_file)
if symbols_df.empty or symbols_df.shape[1] < 1:
    raise ValueError(f"{symbols_file} is empty or improperly formatted!")

# Output dataframe
mktcap_df = pd.DataFrame(columns=['symbol', 'market_cap'])

# Start loop
user_agent = ua.random
session = initialize_session(user_agent)

for i, row in symbols_df.iterrows():
    symbol = row.iloc[0]

    # Rotate user agent every 2 symbols
    if i % 2 == 0:
        user_agent = ua.random
        session.close()
        session = initialize_session(user_agent)
        logging.info(f"Rotated user agent at index {i}")
        print(f"Rotated user agent at index {i}")

    market_cap = get_market_cap(symbol, session, user_agent)

    if market_cap:
        print(f"{symbol}: ₹{market_cap}")
        mktcap_df = mktcap_df._append({'symbol': symbol, 'market_cap': market_cap}, ignore_index=True)
    else:
        print(f"Failed to fetch market cap for {symbol}")

    time.sleep(1)

# Close session
session.close()

# Save results
os.makedirs(os.path.dirname(output_file), exist_ok=True)
mktcap_df.to_csv(output_file, index=False)
print(f"Saved market cap data to {output_file}")

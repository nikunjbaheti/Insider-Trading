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

ua = UserAgent()

def get_headers(user_agent, symbol=None):
    referer_url = f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}" if symbol else "https://www.nseindia.com/"
    return {
        "User-Agent": user_agent,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "*/*",
        "Referer": referer_url
    }

def warm_up_session(session, user_agent, symbol):
    try:
        homepage = "https://www.nseindia.com"
        session.get(homepage, headers=get_headers(user_agent), timeout=10)
        time.sleep(1)

        quote_url = f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
        session.get(quote_url, headers=get_headers(user_agent, symbol), timeout=10)
        time.sleep(1)
    except Exception as e:
        logging.error(f"Error warming up session for {symbol}: {str(e)}")
        print(f"Error warming up session for {symbol}: {str(e)}")

def get_market_cap(symbol, session, user_agent):
    try:
        url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}&section=trade_info"
        response = session.get(url, headers=get_headers(user_agent, symbol), timeout=10)

        if response.status_code == 200:
            try:
                data = response.json()
                market_cap = data.get("marketDeptOrderBook", {}).get("tradeInfo", {}).get("totalMarketCap", None)
                return market_cap
            except Exception as e_json:
                logging.error(f"JSON decode error for {symbol}: {str(e_json)}")
                logging.error(f"Response text for {symbol}: {response.text[:500]}")
                print(f"JSON decode error for {symbol}: {str(e_json)}")
                print(f"Response text: {response.text[:500]}")
                return None
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

if not os.path.exists(symbols_file):
    raise FileNotFoundError(f"{symbols_file} not found!")

symbols_df = pd.read_csv(symbols_file)
if symbols_df.empty or symbols_df.shape[1] < 1:
    raise ValueError(f"{symbols_file} is empty or improperly formatted!")

mktcap_df = pd.DataFrame(columns=['symbol', 'market_cap'])

user_agent = ua.random
session = requests.Session()

for i, row in symbols_df.iterrows():
    symbol = row.iloc[0]

    # Rotate user agent every 2 symbols
    if i % 2 == 0:
        user_agent = ua.random
        session.close()
        session = requests.Session()
        logging.info(f"Rotated user agent at index {i}")
        print(f"Rotated user agent at index {i}")

    warm_up_session(session, user_agent, symbol)

    market_cap = get_market_cap(symbol, session, user_agent)

    if market_cap:
        print(f"{symbol}: ₹{market_cap}")
        mktcap_df = mktcap_df._append({'symbol': symbol, 'market_cap': market_cap}, ignore_index=True)
    else:
        print(f"Failed to fetch market cap for {symbol}")

    time.sleep(1)

session.close()

os.makedirs(os.path.dirname(output_file), exist_ok=True)
mktcap_df.to_csv(output_file, index=False)
print(f"Saved market cap data to {output_file}")

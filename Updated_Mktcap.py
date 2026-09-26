import os
import time
import logging
import pandas as pd
import requests
from fake_useragent import UserAgent

# ============================================================
# CONFIGURATION
# ============================================================

logging.basicConfig(
    filename='/home/nikunj/NseInsiderTrading/log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

ua = UserAgent()

SYMBOLS_FILE = '/home/nikunj/NseInsiderTrading/Symbols.csv'
OUTPUT_FILE = '/home/nikunj/NseInsiderTrading/mktcap.csv'

NSE_HOME = "https://www.nseindia.com"

# ============================================================
# HEADERS
# ============================================================

def get_headers(user_agent, symbol=None):
    referer_url = (
        f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
        if symbol
        else NSE_HOME
    )

    return {
        "User-Agent": user_agent,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "application/json, text/plain, */*",
        "Referer": referer_url,
        "Connection": "keep-alive"
    }


# ============================================================
# WARM UP NSE SESSION
# ============================================================

def warm_up_session(session, user_agent, symbol):
    try:
        # 1. NSE Homepage
        session.get(
            NSE_HOME,
            headers=get_headers(user_agent),
            timeout=10
        )

        time.sleep(1)

        # 2. Equity Quote Page
        quote_url = (
            f"https://www.nseindia.com/get-quotes/equity"
            f"?symbol={symbol}"
        )

        session.get(
            quote_url,
            headers=get_headers(user_agent, symbol),
            timeout=10
        )

        time.sleep(1)

        return True

    except Exception as e:
        error_msg = (
            f"Error warming up session for {symbol}: {str(e)}"
        )

        logging.error(error_msg)
        print(error_msg)

        return False


# ============================================================
# GET MARKET CAP - NEW NSE API
# ============================================================

def get_market_cap(symbol, session, user_agent):
    try:

        url = (
            "https://www.nseindia.com/api/NextApi/apiClient/"
            "GetQuoteApi"
            f"?functionName=getSymbolData"
            f"&marketType=N"
            f"&series=EQ"
            f"&symbol={symbol}"
        )

        response = session.get(
            url,
            headers=get_headers(user_agent, symbol),
            timeout=10
        )

        # ----------------------------------------------------
        # HTTP STATUS CHECK
        # ----------------------------------------------------

        if response.status_code != 200:

            msg = (
                f"{symbol} failed with status code "
                f"{response.status_code}"
            )

            logging.error(msg)
            print(msg)

            return None

        # ----------------------------------------------------
        # JSON RESPONSE
        # ----------------------------------------------------

        try:
            data = response.json()

        except Exception as e_json:

            logging.error(
                f"JSON decode error for {symbol}: {str(e_json)}"
            )

            logging.error(
                f"Response text for {symbol}: "
                f"{response.text[:500]}"
            )

            print(
                f"JSON decode error for {symbol}: "
                f"{str(e_json)}"
            )

            return None

        # ----------------------------------------------------
        # EXTRACT MARKET CAP
        # ----------------------------------------------------

        equity_response = data.get("equityResponse", [])

        if not equity_response:

            msg = f"No equityResponse found for {symbol}"

            logging.error(msg)
            print(msg)

            return None

        trade_info = equity_response[0].get("tradeInfo", {})

        market_cap = trade_info.get("totalMarketCap")

        if market_cap is None:

            msg = f"totalMarketCap not found for {symbol}"

            logging.error(msg)
            print(msg)

            return None

        return market_cap

    except Exception as e:

        error_msg = (
            f"Error fetching market cap for {symbol}: "
            f"{str(e)}"
        )

        logging.error(error_msg)
        print(error_msg)

        return None


# ============================================================
# VALIDATE INPUT FILE
# ============================================================

if not os.path.exists(SYMBOLS_FILE):
    raise FileNotFoundError(
        f"{SYMBOLS_FILE} not found!"
    )

symbols_df = pd.read_csv(SYMBOLS_FILE)

if symbols_df.empty or symbols_df.shape[1] < 1:
    raise ValueError(
        f"{SYMBOLS_FILE} is empty or improperly formatted!"
    )


# ============================================================
# OUTPUT DATAFRAME
# ============================================================

mktcap_data = []


# ============================================================
# CREATE SESSION
# ============================================================

user_agent = ua.random
session = requests.Session()


# ============================================================
# PROCESS SYMBOLS
# ============================================================

for i, row in symbols_df.iterrows():

    symbol = str(row.iloc[0]).strip().upper()

    if not symbol:
        continue

    # --------------------------------------------------------
    # ROTATE USER AGENT + SESSION EVERY 2 SYMBOLS
    # --------------------------------------------------------

    if i % 2 == 0:

        user_agent = ua.random

        try:
            session.close()
        except:
            pass

        session = requests.Session()

        logging.info(
            f"Rotated user agent at index {i}"
        )

        print(
            f"Rotated user agent at index {i}"
        )

    # --------------------------------------------------------
    # WARM UP SESSION
    # --------------------------------------------------------

    warm_up_session(
        session,
        user_agent,
        symbol
    )

    # --------------------------------------------------------
    # GET MARKET CAP
    # --------------------------------------------------------

    market_cap = get_market_cap(
        symbol,
        session,
        user_agent
    )

    if market_cap is not None:

        print(
            f"{symbol}: ₹{market_cap:,.2f}"
        )

        mktcap_data.append({
            'symbol': symbol,
            'market_cap': market_cap
        })

        logging.info(
            f"{symbol}: Market Cap = {market_cap}"
        )

    else:

        print(
            f"Failed to fetch market cap for {symbol}"
        )

        logging.error(
            f"Failed to fetch market cap for {symbol}"
        )

    # --------------------------------------------------------
    # DELAY BETWEEN REQUESTS
    # --------------------------------------------------------

    time.sleep(1)


# ============================================================
# CLOSE SESSION
# ============================================================

session.close()


# ============================================================
# SAVE OUTPUT
# ============================================================

mktcap_df = pd.DataFrame(
    mktcap_data,
    columns=['symbol', 'market_cap']
)

os.makedirs(
    os.path.dirname(OUTPUT_FILE),
    exist_ok=True
)

mktcap_df.to_csv(
    OUTPUT_FILE,
    index=False
)

print(
    f"Saved market cap data to {OUTPUT_FILE}"
)

logging.info(
    f"Saved {len(mktcap_df)} market cap records "
    f"to {OUTPUT_FILE}"
)

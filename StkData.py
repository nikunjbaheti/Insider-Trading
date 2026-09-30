import os
import time
import logging
import requests
import pandas as pd
from fake_useragent import UserAgent


# ============================================================
# CONFIGURATION
# ============================================================

LOG_FILE = '/home/nikunj/NseInsiderTrading/log.txt'
INPUT_FILE = '/home/nikunj/NseInsiderTrading/Symbols.csv'
OUTPUT_DIRECTORY = '/home/nikunj/NseInsiderTrading'
OUTPUT_FILE = os.path.join(OUTPUT_DIRECTORY, 'stkdata.csv')

NSE_HOME = 'https://www.nseindia.com'

# Number of symbols to process before rotating session
SESSION_ROTATE_EVERY = 50

# Normal delay between symbols
REQUEST_DELAY = 2

# Maximum retries for 403 / 429
MAX_RETRIES = 4


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def log_and_print(message, level="info"):

    if level.lower() == "error":
        logging.error(message)

    elif level.lower() == "warning":
        logging.warning(message)

    else:
        logging.info(message)

    print(message)


# ============================================================
# USER AGENT
# ============================================================

ua = UserAgent()


# ============================================================
# HEADERS
# ============================================================

def get_headers(user_agent, symbol=None):

    if symbol:

        referer_url = (
            f'https://www.nseindia.com/get-quotes/equity'
            f'?symbol={symbol}'
        )

    else:
        referer_url = NSE_HOME

    return {
        'User-Agent': user_agent,
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': referer_url,
        'Origin': 'https://www.nseindia.com',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin'
    }


# ============================================================
# CREATE NEW SESSION
# ============================================================

def create_session():

    user_agent = ua.random

    session = requests.Session()

    session.headers.update({
        'User-Agent': user_agent,
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    })

    log_and_print(
        f"Created new NSE session"
    )

    log_and_print(
        f"User-Agent: {user_agent}"
    )

    return session, user_agent


# ============================================================
# ESTABLISH SESSION
# ============================================================

def establish_session(session, symbol, user_agent):

    try:

        # ----------------------------------------------------
        # 1. NSE HOMEPAGE
        # ----------------------------------------------------

        log_and_print(
            f"Visiting NSE homepage for {symbol}"
        )

        response = session.get(
            NSE_HOME,
            headers=get_headers(user_agent),
            timeout=15
        )

        log_and_print(
            f"NSE homepage status for {symbol}: "
            f"{response.status_code}"
        )

        # ----------------------------------------------------
        # IMPORTANT:
        # Do NOT fail if homepage returns 403.
        # We will still try the actual API.
        # ----------------------------------------------------

        if response.status_code == 200:

            time.sleep(1)

        elif response.status_code == 403:

            log_and_print(
                f"NSE homepage returned 403 for {symbol}. "
                f"Continuing with API request.",
                level="warning"
            )

        else:

            log_and_print(
                f"NSE homepage returned "
                f"{response.status_code} for {symbol}. "
                f"Continuing.",
                level="warning"
            )

        # ----------------------------------------------------
        # 2. EQUITY QUOTE PAGE
        # ----------------------------------------------------

        quote_url = (
            f'https://www.nseindia.com/get-quotes/equity'
            f'?symbol={symbol}'
        )

        log_and_print(
            f"Visiting equity quote page for {symbol}"
        )

        response = session.get(
            quote_url,
            headers=get_headers(
                user_agent,
                symbol
            ),
            timeout=15
        )

        log_and_print(
            f"Equity page status for {symbol}: "
            f"{response.status_code}"
        )

        # Again, don't make this a hard failure.

        if response.status_code == 403:

            log_and_print(
                f"Equity page returned 403 for {symbol}. "
                f"Continuing with API request.",
                level="warning"
            )

        time.sleep(1)

        return True

    except requests.exceptions.Timeout:

        log_and_print(
            f"Timeout while establishing session for {symbol}",
            level="warning"
        )

        return True

    except requests.exceptions.RequestException as e:

        log_and_print(
            f"Request error while establishing session "
            f"for {symbol}: {str(e)}",
            level="warning"
        )

        return True

    except Exception as e:

        log_and_print(
            f"Exception during session establishment "
            f"for {symbol}: {str(e)}",
            level="warning"
        )

        return True


# ============================================================
# BUILD NEW NSE API URL
# ============================================================

def get_api_url(symbol):

    return (
        'https://www.nseindia.com/api/NextApi/apiClient/'
        'GetQuoteApi'
        '?functionName=getSymbolData'
        '&marketType=N'
        '&series=EQ'
        f'&symbol={symbol}'
    )


# ============================================================
# GET STOCK DATA
# ============================================================

def get_stock_data(
    symbol,
    session,
    user_agent
):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            # ------------------------------------------------
            # Establish session
            # ------------------------------------------------

            establish_session(
                session,
                symbol,
                user_agent
            )

            # ------------------------------------------------
            # New NSE API
            # ------------------------------------------------

            api_url = get_api_url(symbol)

            log_and_print(
                f"Fetching new NSE API for {symbol} "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )

            response = session.get(
                api_url,
                headers=get_headers(
                    user_agent,
                    symbol
                ),
                timeout=15
            )

            status_code = response.status_code

            log_and_print(
                f"API status for {symbol}: "
                f"{status_code}"
            )

            # =================================================
            # SUCCESS
            # =================================================

            if status_code == 200:

                # ---------------------------------------------
                # Check JSON
                # ---------------------------------------------

                try:

                    data = response.json()

                except Exception as e:

                    log_and_print(
                        f"JSON decode error for {symbol}: "
                        f"{str(e)}",
                        level="error"
                    )

                    log_and_print(
                        f"Response: "
                        f"{response.text[:500]}",
                        level="error"
                    )

                    return None

                # ---------------------------------------------
                # equityResponse
                # ---------------------------------------------

                equity_response = data.get(
                    'equityResponse',
                    []
                )

                if not equity_response:

                    log_and_print(
                        f"No equityResponse found for {symbol}",
                        level="warning"
                    )

                    return None

                # ---------------------------------------------
                # secInfo
                # ---------------------------------------------

                sec_info = equity_response[0].get(
                    'secInfo',
                    {}
                )

                basic_industry = sec_info.get(
                    'basicIndustry'
                )

                industry = sec_info.get(
                    'industryInfo'
                )

                macro = sec_info.get(
                    'macro'
                )

                sector = sec_info.get(
                    'sector'
                )

                # ---------------------------------------------
                # Validate
                # ---------------------------------------------

                if all(
                    value is None
                    for value in [
                        basic_industry,
                        industry,
                        macro,
                        sector
                    ]
                ):

                    log_and_print(
                        f"No industry information found "
                        f"for {symbol}",
                        level="warning"
                    )

                    return None

                # ---------------------------------------------
                # Success
                # ---------------------------------------------

                log_and_print(
                    f"Successfully retrieved data "
                    f"for {symbol}"
                )

                return {
                    'symbol': symbol,
                    'basic_industry': basic_industry,
                    'industry': industry,
                    'macro': macro,
                    'sector': sector
                }

            # =================================================
            # 403 - FORBIDDEN
            # =================================================

            elif status_code == 403:

                log_and_print(
                    f"403 Forbidden for {symbol}. "
                    f"Recreating session.",
                    level="warning"
                )

                # Close current session
                try:
                    session.close()
                except Exception:
                    pass

                # Create completely new session
                session, user_agent = create_session()

                # Wait before retry
                wait_time = attempt * 3

                log_and_print(
                    f"Waiting {wait_time} seconds "
                    f"before retrying {symbol}"
                )

                time.sleep(wait_time)

                continue

            # =================================================
            # 429 - RATE LIMIT
            # =================================================

            elif status_code == 429:

                wait_time = attempt * 10

                log_and_print(
                    f"429 Rate Limited for {symbol}. "
                    f"Waiting {wait_time} seconds.",
                    level="warning"
                )

                time.sleep(wait_time)

                continue

            # =================================================
            # OTHER ERROR
            # =================================================

            else:

                log_and_print(
                    f"API failed for {symbol}. "
                    f"Status: {status_code}",
                    level="error"
                )

                log_and_print(
                    f"Response: "
                    f"{response.text[:500]}",
                    level="error"
                )

                return None

        except requests.exceptions.Timeout:

            log_and_print(
                f"Timeout for {symbol}, "
                f"attempt {attempt}/{MAX_RETRIES}",
                level="warning"
            )

            time.sleep(attempt * 3)

        except requests.exceptions.RequestException as e:

            log_and_print(
                f"Request error for {symbol}: {str(e)}",
                level="warning"
            )

            time.sleep(attempt * 3)

        except Exception as e:

            log_and_print(
                f"Unexpected error for {symbol}: {str(e)}",
                level="error"
            )

            return None

    # --------------------------------------------------------
    # All retries exhausted
    # --------------------------------------------------------

    log_and_print(
        f"Failed to retrieve data for {symbol} "
        f"after {MAX_RETRIES} attempts",
        level="error"
    )

    return None


# ============================================================
# MAIN
# ============================================================

def main():

    # --------------------------------------------------------
    # Validate input
    # --------------------------------------------------------

    if not os.path.exists(INPUT_FILE):

        raise FileNotFoundError(
            f"{INPUT_FILE} not found!"
        )

    symbols_df = pd.read_csv(
        INPUT_FILE
    )

    if symbols_df.empty:

        raise ValueError(
            f"{INPUT_FILE} is empty!"
        )

    if symbols_df.shape[1] < 1:

        raise ValueError(
            f"{INPUT_FILE} has no symbol column!"
        )

    # --------------------------------------------------------
    # Output directory
    # --------------------------------------------------------

    os.makedirs(
        OUTPUT_DIRECTORY,
        exist_ok=True
    )

    # --------------------------------------------------------
    # Create initial session
    # --------------------------------------------------------

    session, user_agent = create_session()

    # --------------------------------------------------------
    # Results
    # --------------------------------------------------------

    stkdata = []

    total_symbols = len(symbols_df)

    log_and_print(
        f"Starting processing of "
        f"{total_symbols} symbols"
    )

    # ========================================================
    # PROCESS SYMBOLS
    # ========================================================

    for index, row in symbols_df.iterrows():

        symbol = str(
            row.iloc[0]
        ).strip().upper()

        if not symbol:

            continue

        # ----------------------------------------------------
        # Rotate session after fixed number of symbols
        # ----------------------------------------------------

        if (
            index > 0
            and index % SESSION_ROTATE_EVERY == 0
        ):

            log_and_print(
                f"Rotating session after "
                f"{SESSION_ROTATE_EVERY} symbols"
            )

            try:
                session.close()
            except Exception:
                pass

            session, user_agent = create_session()

        # ----------------------------------------------------
        # Process
        # ----------------------------------------------------

        log_and_print(
            f"Processing symbol "
            f"{index + 1}/{total_symbols}: "
            f"{symbol}"
        )

        stock_data = get_stock_data(
            symbol,
            session,
            user_agent
        )

        if stock_data:

            stkdata.append(
                stock_data
            )

            log_and_print(
                f"Data retrieved: {symbol} | "
                f"Basic Industry: "
                f"{stock_data['basic_industry']} | "
                f"Industry: "
                f"{stock_data['industry']} | "
                f"Macro: "
                f"{stock_data['macro']} | "
                f"Sector: "
                f"{stock_data['sector']}"
            )

        else:

            log_and_print(
                f"No data found for {symbol}",
                level="warning"
            )

        # ----------------------------------------------------
        # Delay
        # ----------------------------------------------------

        time.sleep(
            REQUEST_DELAY
        )

    # ========================================================
    # CLOSE SESSION
    # ========================================================

    try:
        session.close()
    except Exception:
        pass

    # ========================================================
    # CREATE DATAFRAME
    # ========================================================

    columns = [
        'symbol',
        'basic_industry',
        'industry',
        'macro',
        'sector'
    ]

    stkdata_df = pd.DataFrame(
        stkdata,
        columns=columns
    )

    # ========================================================
    # SAVE
    # ========================================================

    log_and_print(
        f"Saving data to {OUTPUT_FILE}"
    )

    stkdata_df.to_csv(
        OUTPUT_FILE,
        index=False
    )

    # ========================================================
    # SUMMARY
    # ========================================================

    log_and_print(
        f"Data saved successfully"
    )

    log_and_print(
        f"Total symbols: {total_symbols}"
    )

    log_and_print(
        f"Successful: {len(stkdata_df)}"
    )

    log_and_print(
        f"Failed: "
        f"{total_symbols - len(stkdata_df)}"
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()

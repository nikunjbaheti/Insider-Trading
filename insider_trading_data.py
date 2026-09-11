import os
import time
import logging
import requests
import pandas as pd
import xml.etree.ElementTree as ET

from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# SETTINGS
# ============================================================

OUTPUT_DIRECTORY = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "NseInsiderTrading"
)

XML_DIRECTORY = os.path.join(
    OUTPUT_DIRECTORY,
    "xml"
)

OUTPUT_FILE = os.path.join(
    OUTPUT_DIRECTORY,
    "insider.csv"
)

LOOKBACK_DAYS = 120
CHUNK_DAYS = 7
MAX_WORKERS = 5

API_URL = "https://www.nseindia.com/api/corporates-pit-gg"

NSE_HOME = "https://www.nseindia.com"

INSIDER_PAGE = (
    "https://www.nseindia.com/"
    "companies-listing/corporate-filings-insider-trading"
)


# ============================================================
# EXACT EXISTING OUTPUT COLUMNS
# DO NOT CHANGE THIS LIST
# ============================================================

OLD_COLUMNS = [
    "symbol",
    "company",
    "anex",
    "acqName",
    "date",
    "pid",
    "buyValue",
    "sellValue",
    "buyQuantity",
    "sellquantity",
    "secType",
    "secAcq",
    "tdpTransactionType",
    "xbrl",
    "personCategory",
    "befAcqSharesNo",
    "befAcqSharesPer",
    "secVal",
    "securitiesTypePost",
    "afterAcqSharesNo",
    "afterAcqSharesPer",
    "acqfromDt",
    "acqtoDt",
    "intimDt",
    "acqMode",
    "derivativeType",
    "exchange",
    "remarks"
]


# ============================================================
# LOGGING
# ============================================================

os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
os.makedirs(XML_DIRECTORY, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# NSE HEADERS
# ============================================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,"
        "application/xml;q=0.9,image/avif,image/webp,"
        "*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}


# ============================================================
# CREATE SESSION
# ============================================================

def create_session():

    session = requests.Session()

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=10
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(HEADERS)

    return session


# ============================================================
# WARM UP NSE SESSION
# ============================================================

def warm_up_session(session):

    try:

        response = session.get(
            NSE_HOME,
            timeout=30
        )

        logger.info(
            f"NSE homepage: {response.status_code}"
        )

    except Exception as e:

        logger.warning(
            f"NSE homepage failed: {e}"
        )

    try:

        response = session.get(
            INSIDER_PAGE,
            timeout=30
        )

        logger.info(
            f"Insider page: {response.status_code}"
        )

    except Exception as e:

        logger.warning(
            f"Insider page failed: {e}"
        )

    logger.info(
        f"Cookies: {session.cookies.get_dict()}"
    )


# ============================================================
# GET NSE API DATA
# ============================================================

def get_data(session, start_date, end_date):

    params = {
        "index": "equities",
        "from_date": start_date.strftime("%d-%m-%Y"),
        "to_date": end_date.strftime("%d-%m-%Y")
    }

    logger.info(
        f"Requesting: "
        f"{params['from_date']} -> {params['to_date']}"
    )

    try:

        response = session.get(
            API_URL,
            params=params,
            timeout=60
        )

        logger.info(
            f"HTTP: {response.status_code}"
        )

        response.raise_for_status()

        result = response.json()

        data = result.get("data", [])

        logger.info(
            f"Records: {len(data)}"
        )

        return data

    except Exception as e:

        logger.error(
            f"API error: {e}"
        )

        return []


# ============================================================
# GET ALL FILINGS
# ============================================================

def get_all_filings():

    session = create_session()

    warm_up_session(session)

    today = datetime.now().date()

    start_date = today - timedelta(
        days=LOOKBACK_DAYS
    )

    end_date = today

    all_records = []

    current_end = end_date

    while current_end > start_date:

        current_start = max(
            start_date,
            current_end - timedelta(
                days=CHUNK_DAYS - 1
            )
        )

        records = get_data(
            session,
            current_start,
            current_end
        )

        all_records.extend(records)

        current_end = (
            current_start - timedelta(days=1)
        )

        time.sleep(1)

    logger.info(
        f"Total records collected: "
        f"{len(all_records)}"
    )

    return all_records


# ============================================================
# XML HELPER
# ============================================================

def clean_tag(tag):

    """
    Remove XML namespace.

    Example:
    {http://example.com}NameOfThePerson
    becomes:
    NameOfThePerson
    """

    if "}" in tag:

        return tag.split("}", 1)[1]

    return tag


# ============================================================
# BUILD XML INDEX
# ============================================================

def build_xml_index(root):

    """
    Creates a dictionary:

        tag name -> list of XML elements

    This makes the parser independent of
    XML namespace prefixes.
    """

    index = {}

    for element in root.iter():

        tag = clean_tag(element.tag)

        index.setdefault(
            tag,
            []
        ).append(element)

    return index


# ============================================================
# GET VALUE FROM XML ELEMENT
# ============================================================

def get_element_value(element):

    if element is None:
        return ""

    if element.text is None:
        return ""

    return element.text.strip()


# ============================================================
# GET FIRST VALUE
# ============================================================

def get_first_value(
    elements,
    tag_name,
    context_ref=None
):

    """
    Find the first matching XML tag.

    If context_ref is provided, only elements
    having that contextRef are considered.
    """

    for element in elements.get(tag_name, []):

        if context_ref is not None:

            element_context = (
                element.attrib.get("contextRef")
                or element.attrib.get("contextref")
            )

            if element_context != context_ref:

                continue

        value = get_element_value(element)

        if value != "":

            return value

    return ""


# ============================================================
# GET ALL DISCLOSURE CONTEXTS
# ============================================================

def get_disclosure_contexts(root):

    """
    Finds Disclosure1, Disclosure2, Disclosure3, etc.
    dynamically.

    This avoids assuming a fixed number of disclosures.
    """

    contexts = set()

    for element in root.iter():

        context_ref = (
            element.attrib.get("contextRef")
            or element.attrib.get("contextref")
        )

        if context_ref and context_ref.lower().startswith(
            "disclosure"
        ):

            contexts.add(context_ref)

    def disclosure_sort_key(value):

        try:

            return int(
                value.lower().replace(
                    "disclosure",
                    ""
                )
            )

        except Exception:

            return 999999

    return sorted(
        contexts,
        key=disclosure_sort_key
    )


# ============================================================
# FIND XML FILE
# ============================================================

def download_xml(session, filing):

    xml_url = filing.get("xmlFileName")

    if not xml_url:

        return None

    app_id = str(
        filing.get("appId", "unknown")
    )

    xml_file = os.path.join(
        XML_DIRECTORY,
        f"{app_id}.xml"
    )

    # --------------------------------------------------------
    # USE CACHE IF ALREADY DOWNLOADED
    # --------------------------------------------------------

    if os.path.exists(xml_file):

        try:

            with open(
                xml_file,
                "rb"
            ) as f:

                return f.read()

        except Exception:

            pass

    # --------------------------------------------------------
    # DOWNLOAD
    # --------------------------------------------------------

    try:

        response = session.get(
            xml_url,
            timeout=60
        )

        response.raise_for_status()

        content = response.content

        with open(
            xml_file,
            "wb"
        ) as f:

            f.write(content)

        return content

    except Exception as e:

        logger.error(
            f"XML download failed "
            f"for appId={app_id}: {e}"
        )

        return None


# ============================================================
# PARSE SINGLE XML DISCLOSURE
# ============================================================

def parse_disclosure(
    root,
    elements,
    context_ref,
    filing
):

    # --------------------------------------------------------
    # FILING LEVEL INFORMATION
    # --------------------------------------------------------

    symbol = get_first_value(
        elements,
        "Symbol",
        "MainI"
    )

    company = get_first_value(
        elements,
        "Company",
        "MainI"
    )

    anex = get_first_value(
        elements,
        "DisclosureUnderRegulation",
        "MainI"
    )

    date = get_first_value(
        elements,
        "DateOfFiling",
        "MainI"
    )

    # --------------------------------------------------------
    # TRANSACTION LEVEL INFORMATION
    # --------------------------------------------------------

    acq_name = get_first_value(
        elements,
        "NameOfThePerson",
        context_ref
    )

    person_category = get_first_value(
        elements,
        "CategoryOfPerson",
        context_ref
    )

    pid = get_first_value(
        elements,
        "IdentificationNumberOfDirectorOrCompany",
        context_ref
    )

    sec_type = get_first_value(
        elements,
        "TypeOfInstrument",
        context_ref
    )

    # --------------------------------------------------------
    # BEFORE HOLDING
    # --------------------------------------------------------

    before_shares = get_first_value(
        elements,
        "SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity",
        context_ref
    )

    before_percentage = get_first_value(
        elements,
        "SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding",
        context_ref
    )

    # --------------------------------------------------------
    # IMPORTANT:
    # TRANSACTION TYPE DIRECTLY FROM XML
    #
    # SecuritiesAcquiredOrDisposedTransactionType
    #
    # Example:
    # Buy
    # Sell
    # --------------------------------------------------------

    transaction_type = get_first_value(
        elements,
        "SecuritiesAcquiredOrDisposedTransactionType",
        context_ref
    )

    # --------------------------------------------------------
    # QUANTITY
    # --------------------------------------------------------

    quantity = get_first_value(
        elements,
        "SecuritiesAcquiredOrDisposedNumberOfSecurity",
        context_ref
    )

    # --------------------------------------------------------
    # VALUE
    # --------------------------------------------------------

    transaction_value = get_first_value(
        elements,
        "SecuritiesAcquiredOrDisposedValueOfSecurity",
        context_ref
    )

    # --------------------------------------------------------
    # AFTER HOLDING
    # --------------------------------------------------------

    after_shares = get_first_value(
        elements,
        "SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity",
        context_ref
    )

    after_percentage = get_first_value(
        elements,
        "SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding",
        context_ref
    )

    # --------------------------------------------------------
    # TRANSACTION DATES
    # --------------------------------------------------------

    from_date = get_first_value(
        elements,
        "DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate",
        context_ref
    )

    to_date = get_first_value(
        elements,
        "DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate",
        context_ref
    )

    intimation_date = get_first_value(
        elements,
        "DateOfIntimationToCompany",
        context_ref
    )

    # --------------------------------------------------------
    # MODE
    # --------------------------------------------------------

    acquisition_mode = get_first_value(
        elements,
        "ModeOfAcquisitionOrDisposal",
        context_ref
    )

    # --------------------------------------------------------
    # EXCHANGE
    # --------------------------------------------------------

    exchange = get_first_value(
        elements,
        "ExchangeOnWhichTheTradeWasExecuted",
        context_ref
    )

    # --------------------------------------------------------
    # DERIVATIVE
    # --------------------------------------------------------

    derivative_type = get_first_value(
        elements,
        "DerivativeType",
        context_ref
    )

    # --------------------------------------------------------
    # SECURITIES TYPE POST
    # --------------------------------------------------------

    securities_type_post = get_first_value(
        elements,
        "SecuritiesTypePost",
        context_ref
    )

    # --------------------------------------------------------
    # REMARKS
    # --------------------------------------------------------

    remarks = get_first_value(
        elements,
        "Remarks",
        context_ref
    )

    # --------------------------------------------------------
    # BUY / SELL
    #
    # We use the XML transaction type directly.
    # --------------------------------------------------------

    buy_value = ""
    sell_value = ""

    buy_quantity = ""
    sell_quantity = ""

    normalized_transaction_type = (
        transaction_type.strip().lower()
        if transaction_type
        else ""
    )

    if normalized_transaction_type == "buy":

        buy_value = transaction_value
        buy_quantity = quantity

    elif normalized_transaction_type == "sell":

        sell_value = transaction_value
        sell_quantity = quantity

    # --------------------------------------------------------
    # CREATE OUTPUT ROW
    # --------------------------------------------------------

    row = {

        "symbol": symbol,

        "company": company,

        "anex": anex,

        "acqName": acq_name,

        "date": date,

        "pid": pid,

        "buyValue": buy_value,

        "sellValue": sell_value,

        "buyQuantity": buy_quantity,

        "sellquantity": sell_quantity,

        "secType": sec_type,

        # Preserving the existing output structure.
        # Quantity of securities involved in transaction.
        "secAcq": quantity,

        # IMPORTANT:
        # Directly from:
        # SecuritiesAcquiredOrDisposedTransactionType
        "tdpTransactionType": transaction_type,

        # API ixbrl URL
        "xbrl": filing.get("ixbrl", ""),

        "personCategory": person_category,

        "befAcqSharesNo": before_shares,

        "befAcqSharesPer": before_percentage,

        "secVal": transaction_value,

        "securitiesTypePost": securities_type_post,

        "afterAcqSharesNo": after_shares,

        "afterAcqSharesPer": after_percentage,

        "acqfromDt": from_date,

        "acqtoDt": to_date,

        "intimDt": intimation_date,

        "acqMode": acquisition_mode,

        "derivativeType": derivative_type,

        "exchange": exchange,

        "remarks": remarks
    }

    return row


# ============================================================
# PARSE XML
# ============================================================

def parse_xml(
    xml_content,
    filing
):

    try:

        root = ET.fromstring(
            xml_content
        )

    except Exception as e:

        logger.error(
            f"XML parsing failed "
            f"for appId={filing.get('appId')}: {e}"
        )

        return []

    elements = build_xml_index(
        root
    )

    disclosure_contexts = (
        get_disclosure_contexts(root)
    )

    rows = []

    # --------------------------------------------------------
    # PROCESS EACH DISCLOSURE
    # --------------------------------------------------------

    for context_ref in disclosure_contexts:

        row = parse_disclosure(
            root,
            elements,
            context_ref,
            filing
        )

        # ----------------------------------------------------
        # ONLY ADD IF THERE IS ACTUAL TRANSACTION DATA
        # ----------------------------------------------------

        if (
            row["acqName"]
            or row["tdpTransactionType"]
            or row["secVal"]
            or row["buyQuantity"]
            or row["sellquantity"]
        ):

            rows.append(row)

    return rows


# ============================================================
# PROCESS ONE FILING
# ============================================================

def process_filing(filing):

    session = create_session()

    xml_content = download_xml(
        session,
        filing
    )

    if xml_content is None:

        return []

    rows = parse_xml(
        xml_content,
        filing
    )

    return rows


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()

    logger.info(
        "Starting NSE Insider Trading scraper"
    )

    # --------------------------------------------------------
    # GET FILINGS FROM NSE API
    # --------------------------------------------------------

    filings = get_all_filings()

    if not filings:

        logger.error(
            "No filings received from NSE API."
        )

        return

    logger.info(
        f"Got {len(filings)} filings"
    )

    # --------------------------------------------------------
    # SHOW FIRST FILING
    # --------------------------------------------------------

    logger.info(
        f"First filing:\n{filings[0]}"
    )

    # --------------------------------------------------------
    # PROCESS XML FILES IN PARALLEL
    # --------------------------------------------------------

    all_rows = []

    completed = 0

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                process_filing,
                filing
            ): filing
            for filing in filings
        }

        for future in as_completed(
            futures
        ):

            filing = futures[future]

            try:

                rows = future.result()

                all_rows.extend(rows)

            except Exception as e:

                logger.error(
                    f"Processing failed "
                    f"for appId={filing.get('appId')}: {e}"
                )

            completed += 1

            if (
                completed % 100 == 0
                or completed == len(filings)
            ):

                logger.info(
                    f"Processed "
                    f"{completed}/{len(filings)} filings"
                )

    # --------------------------------------------------------
    # CREATE DATAFRAME
    # --------------------------------------------------------

    if not all_rows:

        logger.warning(
            "No transaction rows extracted."
        )

        return

    df = pd.DataFrame(
        all_rows
    )

    # --------------------------------------------------------
    # FORCE EXACT EXISTING COLUMN STRUCTURE
    #
    # This guarantees that no additional columns are
    # introduced even if NSE adds new XML fields.
    # --------------------------------------------------------

    for column in OLD_COLUMNS:

        if column not in df.columns:

            df[column] = ""

    df = df[
        OLD_COLUMNS
    ]

    # --------------------------------------------------------
    # CLEAN STRING COLUMNS
    # --------------------------------------------------------

    for column in df.columns:

        df[column] = (
            df[column]
            .fillna("")
            .astype(str)
            .str.strip()
        )

    # --------------------------------------------------------
    # NUMERIC COLUMNS
    # --------------------------------------------------------

    numeric_columns = [
        "buyValue",
        "sellValue",
        "buyQuantity",
        "sellquantity",
        "secAcq",
        "befAcqSharesNo",
        "befAcqSharesPer",
        "secVal",
        "afterAcqSharesNo",
        "afterAcqSharesPer"
    ]

    for column in numeric_columns:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    # --------------------------------------------------------
    # REMOVE DUPLICATE TRANSACTIONS
    #
    # Keep this conservative. We only remove exact duplicate
    # rows rather than potentially removing legitimate
    # transactions.
    # --------------------------------------------------------

    before_duplicates = len(df)

    df = df.drop_duplicates()

    removed_duplicates = (
        before_duplicates - len(df)
    )

    logger.info(
        f"Removed exact duplicate rows: "
        f"{removed_duplicates}"
    )

    # --------------------------------------------------------
    # SAVE CSV
    # --------------------------------------------------------

    df.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    elapsed = (
        time.time() - start_time
    )

    logger.info(
        "================================================"
    )

    logger.info(
        "SCRAPING COMPLETED"
    )

    logger.info(
        f"Filings received       : {len(filings)}"
    )

    logger.info(
        f"Transactions extracted : {len(df)}"
    )

    logger.info(
        f"Columns                 : {len(df.columns)}"
    )

    logger.info(
        f"Output                  : {OUTPUT_FILE}"
    )

    logger.info(
        f"Time taken              : {elapsed:.2f} seconds"
    )

    logger.info(
        "================================================"
    )

    # --------------------------------------------------------
    # TRANSACTION TYPE SUMMARY
    # --------------------------------------------------------

    if "tdpTransactionType" in df.columns:

        logger.info(
            "Transaction Type Summary:"
        )

        logger.info(
            "\n"
            + df[
                "tdpTransactionType"
            ]
            .value_counts(
                dropna=False
            )
            .to_string()
        )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()

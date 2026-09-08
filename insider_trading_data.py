import os
import time
import logging
import requests
import pandas as pd
import xml.etree.ElementTree as ET

from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import local
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# CONFIG
# ============================================================

OUTPUT_DIRECTORY = "/home/nikunj/NseInsiderTrading"

LOOKBACK_DAYS = 120

CHUNK_DAYS = 7

MAX_WORKERS = 5

OUTPUT_CSV = os.path.join(
    OUTPUT_DIRECTORY,
    "insider.csv"
)

LOG_FILE_PATH = os.path.join(
    OUTPUT_DIRECTORY,
    "log.txt"
)

XML_DIRECTORY = os.path.join(
    OUTPUT_DIRECTORY,
    "xml"
)

os.makedirs(
    OUTPUT_DIRECTORY,
    exist_ok=True
)

os.makedirs(
    XML_DIRECTORY,
    exist_ok=True
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    filename=LOG_FILE_PATH,
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger(__name__)


# ============================================================
# NSE
# ============================================================

NSE_HOME = (
    "https://www.nseindia.com"
)

NSE_PAGE = (
    "https://www.nseindia.com/"
    "companies-listing/corporate-filings-insider-trading"
)

# THIS IS THE WORKING ENDPOINT
NSE_API = (
    "https://www.nseindia.com/api/corporates-pit-gg"
)


# ============================================================
# USER AGENT
# ============================================================

# Fixed browser UA is more stable than generating
# a different UA on every execution.

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/140.0.0.0 Safari/537.36"
)


def get_headers(referer):

    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer,
        "Origin": NSE_HOME,
        "Connection": "keep-alive"
    }


# ============================================================
# MAIN NSE SESSION
# ============================================================

session = requests.Session()


# ============================================================
# WARM UP NSE
# ============================================================

def warm_up():

    print(
        "Warming up NSE session..."
    )

    try:

        # ----------------------------------------------------
        # NSE homepage
        # ----------------------------------------------------

        r1 = session.get(
            NSE_HOME,
            headers=get_headers(
                NSE_HOME
            ),
            timeout=20
        )

        print(
            f"NSE homepage: "
            f"{r1.status_code}"
        )

        time.sleep(2)

        # ----------------------------------------------------
        # Insider Trading page
        # ----------------------------------------------------

        r2 = session.get(
            NSE_PAGE,
            headers=get_headers(
                NSE_HOME
            ),
            timeout=20
        )

        print(
            f"Insider page: "
            f"{r2.status_code}"
        )

        time.sleep(2)

        print(
            "Cookies:",
            session.cookies.get_dict()
        )

        return True

    except Exception as e:

        print(
            f"Warm-up failed: {e}"
        )

        logger.exception(
            "Warm-up failed"
        )

        return False


# ============================================================
# GET NSE FILINGS
# ============================================================

def get_data():

    end_date = (
        datetime.now().date()
    )

    all_records = []

    # Start from today
    chunk_end = end_date

    # --------------------------------------------------------
    # Query NSE in 7-day chunks
    # --------------------------------------------------------

    while chunk_end >= (
        end_date
        - timedelta(
            days=LOOKBACK_DAYS
        )
    ):

        chunk_start = max(
            chunk_end
            - timedelta(
                days=CHUNK_DAYS - 1
            ),
            end_date
            - timedelta(
                days=LOOKBACK_DAYS
            )
        )

        start_date_str = (
            chunk_start.strftime(
                "%d-%m-%Y"
            )
        )

        end_date_str = (
            chunk_end.strftime(
                "%d-%m-%Y"
            )
        )

        url = (
            f"{NSE_API}"
            f"?index=equities"
            f"&from_date={start_date_str}"
            f"&to_date={end_date_str}"
        )

        print(
            f"\nRequesting: "
            f"{start_date_str} -> "
            f"{end_date_str}"
        )

        try:

            response = session.get(
                url,
                headers=get_headers(
                    NSE_PAGE
                ),
                timeout=30
            )

            print(
                "HTTP:",
                response.status_code
            )

            response.raise_for_status()

            data = response.json()

            records = data.get(
                "data",
                []
            )

            print(
                f"Records: "
                f"{len(records)}"
            )

            if records:

                all_records.extend(
                    records
                )

            # Move backwards
            chunk_end = (
                chunk_start
                - timedelta(
                    days=1
                )
            )

            time.sleep(1)

        except Exception as e:

            print(
                f"Error: {e}"
            )

            logger.exception(
                "Chunk request failed"
            )

            # Continue to previous chunk
            chunk_end = (
                chunk_start
                - timedelta(
                    days=1
                )
            )

    print(
        f"\nTotal records collected: "
        f"{len(all_records)}"
    )

    return {
        "data": all_records
    }


# ============================================================
# XML HELPER
# ============================================================

def clean_tag(tag):

    """
    Remove XML namespace.

    Example:

    {namespace}Symbol
          ↓
    Symbol
    """

    if "}" in tag:

        tag = tag.split(
            "}",
            1
        )[1]

    if ":" in tag:

        tag = tag.split(
            ":",
            1
        )[1]

    return tag


def clean_value(value):

    if value is None:

        return ""

    return str(
        value
    ).strip()


# ============================================================
# PARSE NSE XML
# ============================================================

def parse_xml_contexts(
    xml_path
):

    """
    NSE XML contains:

        MainI
        Disclosure1
        Disclosure2
        Disclosure3
        ...

    MainI contains filing-level fields.

    DisclosureN contains one transaction.
    """

    tree = ET.parse(
        xml_path
    )

    root = tree.getroot()

    main_fields = {}

    disclosures = {}

    for element in root.iter():

        tag = clean_tag(
            element.tag
        )

        context_ref = (
            element.attrib.get(
                "contextRef",
                ""
            )
        )

        value = clean_value(
            element.text
        )

        if not value:

            continue

        # ----------------------------------------------------
        # Filing-level fields
        # ----------------------------------------------------

        if context_ref == "MainI":

            main_fields[
                tag
            ] = value

        # ----------------------------------------------------
        # Transaction-level fields
        # ----------------------------------------------------

        elif context_ref.lower().startswith(
            "disclosure"
        ):

            if context_ref not in disclosures:

                disclosures[
                    context_ref
                ] = {}

            disclosures[
                context_ref
            ][tag] = value

    return (
        main_fields,
        disclosures
    )


# ============================================================
# MAP XML TO EXISTING OUTPUT
# ============================================================

def convert_disclosure_to_old_format(
    disclosure,
    main_fields,
    filing
):

    """
    Converts the NSE XML into the EXACT
    existing output structure.

    DO NOT change these column names.
    """

    row = {

        "symbol": "",
        "company": "",
        "anex": "",
        "acqName": "",
        "date": "",
        "pid": "",
        "buyValue": "",
        "sellValue": "",
        "buyQuantity": "",
        "sellquantity": "",
        "secType": "",
        "secAcq": "",
        "tdpTransactionType": "",
        "xbrl": "",
        "personCategory": "",
        "befAcqSharesNo": "",
        "befAcqSharesPer": "",
        "secVal": "",
        "securitiesTypePost": "",
        "afterAcqSharesNo": "",
        "afterAcqSharesPer": "",
        "acqfromDt": "",
        "acqtoDt": "",
        "intimDt": "",
        "acqMode": "",
        "derivativeType": "",
        "exchange": "",
        "remarks": ""
    }


    # ========================================================
    # FILING LEVEL
    # ========================================================

    row["symbol"] = main_fields.get(
        "Symbol",
        filing.get(
            "symbol",
            ""
        )
    )

    row["company"] = main_fields.get(
        "Company",
        filing.get(
            "companyName",
            ""
        )
    )

    row["anex"] = main_fields.get(
        "DisclosureUnderRegulation",
        filing.get(
            "regulation",
            ""
        )
    )

    row["date"] = main_fields.get(
        "DateOfFiling",
        ""
    )

    row["xbrl"] = filing.get(
        "ixbrl",
        ""
    )


    # ========================================================
    # PERSON
    # ========================================================

    row["acqName"] = disclosure.get(
        "NameOfThePerson",
        ""
    )

    row["personCategory"] = disclosure.get(
        "CategoryOfPerson",
        ""
    )

    row["pid"] = disclosure.get(
        "IdentificationNumberOfDirectorOrCompany",
        ""
    )


    # ========================================================
    # SECURITY TYPE
    # ========================================================

    row["secType"] = disclosure.get(
        "TypeOfInstrument",
        ""
    )


    # ========================================================
    # HOLDING BEFORE
    # ========================================================

    row["befAcqSharesNo"] = disclosure.get(
        "SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity",
        ""
    )

    row["befAcqSharesPer"] = disclosure.get(
        "SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding",
        ""
    )


    # ========================================================
    # TRANSACTION TYPE
    # ========================================================

    transaction_type = disclosure.get(
        "SecuritiesAcquiredOrDisposedTransactionType",
        ""
    )

    row["tdpTransactionType"] = (
        transaction_type
    )


    # ========================================================
    # QUANTITY
    # ========================================================

    quantity = disclosure.get(
        "SecuritiesAcquiredOrDisposedNumberOfSecurity",
        ""
    )


    # ========================================================
    # VALUE
    # ========================================================

    value = disclosure.get(
        "SecuritiesAcquiredOrDisposedValueOfSecurity",
        ""
    )

    row["secVal"] = value


    # ========================================================
    # SECURITIES ACQUIRED / DISPOSED
    # ========================================================

    row["secAcq"] = quantity


    # ========================================================
    # BUY / SELL
    # ========================================================

    transaction_lower = (
        transaction_type
        .strip()
        .lower()
    )

    if transaction_lower == "buy":

        row["buyQuantity"] = quantity

        row["buyValue"] = value

    elif transaction_lower == "sell":

        row["sellquantity"] = quantity

        row["sellValue"] = value


    # ========================================================
    # HOLDING AFTER
    # ========================================================

    row["afterAcqSharesNo"] = disclosure.get(
        "SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity",
        ""
    )

    row["afterAcqSharesPer"] = disclosure.get(
        "SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding",
        ""
    )


    # ========================================================
    # TRANSACTION DATE FROM
    # ========================================================

    row["acqfromDt"] = disclosure.get(
        "DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate",
        ""
    )


    # ========================================================
    # TRANSACTION DATE TO
    # ========================================================

    row["acqtoDt"] = disclosure.get(
        "DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate",
        ""
    )


    # ========================================================
    # INTIMATION DATE
    # ========================================================

    row["intimDt"] = disclosure.get(
        "DateOfIntimationToCompany",
        ""
    )


    # ========================================================
    # MODE OF ACQUISITION / DISPOSAL
    # ========================================================

    row["acqMode"] = disclosure.get(
        "ModeOfAcquisitionOrDisposal",
        ""
    )


    # ========================================================
    # DERIVATIVE TYPE
    # ========================================================

    row["derivativeType"] = disclosure.get(
        "DerivativeType",
        ""
    )


    # ========================================================
    # EXCHANGE
    # ========================================================

    row["exchange"] = disclosure.get(
        "ExchangeOnWhichTheTradeWasExecuted",
        ""
    )


    # ========================================================
    # SECURITY TYPE POST
    # ========================================================

    row["securitiesTypePost"] = disclosure.get(
        "SecuritiesTypePost",
        ""
    )


    # ========================================================
    # REMARKS
    # ========================================================

    row["remarks"] = disclosure.get(
        "Remarks",
        ""
    )


    return row


# ============================================================
# THREAD LOCAL SESSION
# ============================================================

thread_local = local()


def get_thread_session():

    if not hasattr(
        thread_local,
        "session"
    ):

        s = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[
                429,
                500,
                502,
                503,
                504
            ],
            allowed_methods=[
                "GET"
            ]
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy
        )

        s.mount(
            "https://",
            adapter
        )

        # Copy cookies from main session
        for cookie in session.cookies:

            s.cookies.set(
                cookie.name,
                cookie.value,
                domain=cookie.domain,
                path=cookie.path
            )

        thread_local.session = s

    return thread_local.session


# ============================================================
# DOWNLOAD XML
# ============================================================

def download_xml(
    filing
):

    xml_url = filing.get(
        "xmlFileName"
    )

    if not xml_url:

        return (
            filing,
            None,
            "No XML URL"
        )

    app_id = str(
        filing.get(
            "appId",
            ""
        )
    )

    if not app_id:

        return (
            filing,
            None,
            "No appId"
        )

    xml_path = os.path.join(
        XML_DIRECTORY,
        f"{app_id}.xml"
    )


    # --------------------------------------------------------
    # Use cached XML if already downloaded
    # --------------------------------------------------------

    if os.path.exists(
        xml_path
    ):

        return (
            filing,
            xml_path,
            None
        )


    # --------------------------------------------------------
    # Download
    # --------------------------------------------------------

    s = get_thread_session()

    headers = get_headers(
        NSE_PAGE
    )

    headers["Accept"] = (
        "application/xml,"
        "text/xml,"
        "*/*"
    )

    try:

        response = s.get(
            xml_url,
            headers=headers,
            timeout=45
        )

        response.raise_for_status()

        with open(
            xml_path,
            "wb"
        ) as f:

            f.write(
                response.content
            )

        return (
            filing,
            xml_path,
            None
        )

    except Exception as e:

        logger.exception(
            f"XML download failed "
            f"for appId={app_id}"
        )

        return (
            filing,
            None,
            str(e)
        )


# ============================================================
# PROCESS ONE FILING
# ============================================================

def process_filing(
    filing
):

    filing, xml_path, error = (
        download_xml(
            filing
        )
    )

    if error:

        logger.error(
            f"XML failed | "
            f"appId={filing.get('appId')} | "
            f"{error}"
        )

        return []


    try:

        (
            main_fields,
            disclosures
        ) = parse_xml_contexts(
            xml_path
        )


        rows = []


        # ----------------------------------------------------
        # Each Disclosure = One transaction
        # ----------------------------------------------------

        for disclosure_id, disclosure in (
            disclosures.items()
        ):

            row = (
                convert_disclosure_to_old_format(
                    disclosure,
                    main_fields,
                    filing
                )
            )

            rows.append(
                row
            )


        if not rows:

            logger.warning(
                f"No disclosures found | "
                f"appId={filing.get('appId')}"
            )


        return rows


    except Exception as e:

        logger.exception(
            f"XML parsing failed | "
            f"appId={filing.get('appId')}"
        )

        return []


# ============================================================
# EXACT EXISTING COLUMN STRUCTURE
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
# MAIN
# ============================================================

if not warm_up():

    raise SystemExit(
        "Could not warm up NSE session."
    )


# ============================================================
# STEP 1
# GET FILING METADATA
# ============================================================

data = get_data()

records = data.get(
    "data",
    []
)


if not records:

    print(
        "\n⚠️ NSE returned zero filings."
    )

    raise SystemExit()


print(
    f"\n✅ Got {len(records)} filings"
)


# ============================================================
# STEP 2
# REMOVE DUPLICATE APP IDS
# ============================================================

unique_records = {}


for filing in records:

    app_id = filing.get(
        "appId"
    )

    if app_id:

        unique_records[
            str(app_id)
        ] = filing

    else:

        unique_records[
            f"NO_APP_ID_{len(unique_records)}"
        ] = filing


records = list(
    unique_records.values()
)


print(
    f"Unique filings: "
    f"{len(records)}"
)


# ============================================================
# STEP 3
# DOWNLOAD + PARSE XML
# ============================================================

print(
    "\nDownloading and parsing XML files..."
)

all_rows = []

completed = 0

failed = 0

total = len(records)


with ThreadPoolExecutor(
    max_workers=MAX_WORKERS
) as executor:

    futures = {

        executor.submit(
            process_filing,
            filing
        ): filing

        for filing in records

    }


    for future in as_completed(
        futures
    ):

        filing = futures[
            future
        ]

        try:

            rows = future.result()

            if rows:

                all_rows.extend(
                    rows
                )

            else:

                failed += 1

        except Exception as e:

            failed += 1

            logger.exception(
                f"Processing failed | "
                f"appId={filing.get('appId')}"
            )


        completed += 1


        if (
            completed % 25 == 0
            or completed == total
        ):

            print(
                f"Processed "
                f"{completed}/{total} "
                f"| Transactions: "
                f"{len(all_rows)} "
                f"| Failed: "
                f"{failed}"
            )


# ============================================================
# STEP 4
# DATAFRAME
# ============================================================

df = pd.DataFrame(
    all_rows
)


# ============================================================
# STEP 5
# GUARANTEE EXISTING COLUMNS
# ============================================================

for column in OLD_COLUMNS:

    if column not in df.columns:

        df[column] = ""


# ============================================================
# IMPORTANT
#
# FORCE EXACT EXISTING COLUMN ORDER
#
# NO NEW COLUMN CAN APPEAR
# ============================================================

df = df[
    OLD_COLUMNS
]


# ============================================================
# STEP 6
# NUMERIC COLUMNS
# ============================================================

numeric_columns = [

    "buyValue",
    "sellValue",
    "buyQuantity",
    "sellquantity",
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


# ============================================================
# STEP 7
# SAVE CSV
# ============================================================

df.to_csv(
    OUTPUT_CSV,
    index=False,
    encoding="utf-8-sig"
)


# ============================================================
# FINAL SUMMARY
# ============================================================

print()
print(
    "=" * 65
)

print(
    "COMPLETED"
)

print(
    "=" * 65
)

print(
    f"Filings found       : "
    f"{len(records)}"
)

print(
    f"Transactions parsed : "
    f"{len(df)}"
)

print(
    f"Failed filings      : "
    f"{failed}"
)

print(
    f"Output              : "
    f"{OUTPUT_CSV}"
)

print(
    f"XML cache           : "
    f"{XML_DIRECTORY}"
)

print(
    "=" * 65
)

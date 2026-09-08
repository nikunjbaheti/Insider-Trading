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

os.makedirs(
    OUTPUT_DIRECTORY,
    exist_ok=True
)

LOG_FILE_PATH = os.path.join(
    OUTPUT_DIRECTORY,
    "log.txt"
)

OUTPUT_CSV = os.path.join(
    OUTPUT_DIRECTORY,
    "insider.csv"
)

XML_DIRECTORY = os.path.join(
    OUTPUT_DIRECTORY,
    "xml"
)

os.makedirs(
    XML_DIRECTORY,
    exist_ok=True
)

LOOKBACK_DAYS = 120

CHUNK_DAYS = 7

MAX_WORKERS = 5


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

NSE_HOME = "https://www.nseindia.com"

NSE_PAGE = (
    "https://www.nseindia.com/"
    "companies-listing/corporate-filings-insider-trading"
)

NSE_API = (
    "https://www.nseindia.com/api/corporates-pit-gg"
)


# Fixed UA is more stable than fake_useragent
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
        "Connection": "keep-alive",
    }


# ============================================================
# SESSION
# ============================================================

session = requests.Session()


def warm_up():

    print("Warming up NSE session...")

    try:

        r1 = session.get(
            NSE_HOME,
            headers=get_headers(NSE_HOME),
            timeout=20
        )

        print(
            f"NSE homepage: {r1.status_code}"
        )

        time.sleep(2)

        r2 = session.get(
            NSE_PAGE,
            headers=get_headers(NSE_HOME),
            timeout=20
        )

        print(
            f"Insider page: {r2.status_code}"
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

    end_date = datetime.now().date()

    all_records = []

    chunk_end = end_date

    while chunk_end >= end_date - timedelta(
        days=LOOKBACK_DAYS
    ):

        chunk_start = max(
            chunk_end - timedelta(
                days=CHUNK_DAYS - 1
            ),
            end_date - timedelta(
                days=LOOKBACK_DAYS
            )
        )

        start_date_str = chunk_start.strftime(
            "%d-%m-%Y"
        )

        end_date_str = chunk_end.strftime(
            "%d-%m-%Y"
        )

        url = (
            f"{NSE_API}"
            f"?index=equities"
            f"&from_date={start_date_str}"
            f"&to_date={end_date_str}"
        )

        print(
            f"\nRequesting: "
            f"{start_date_str} -> {end_date_str}"
        )

        try:

            response = session.get(
                url,
                headers=get_headers(NSE_PAGE),
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
                f"Records: {len(records)}"
            )

            if records:

                all_records.extend(
                    records
                )

            chunk_end = (
                chunk_start
                - timedelta(days=1)
            )

            time.sleep(1)

        except Exception as e:

            print(
                f"Error: {e}"
            )

            logger.exception(
                "Chunk request failed"
            )

            chunk_end = (
                chunk_start
                - timedelta(days=1)
            )

    print(
        f"\nTotal records collected: "
        f"{len(all_records)}"
    )

    return {
        "data": all_records
    }


# ============================================================
# XML HELPERS
# ============================================================

def clean_tag(tag):

    """
    Removes XML namespace.

    Example:
        {http://www.xbrl.org/...}Symbol
        ->
        Symbol
    """

    if "}" in tag:

        tag = tag.split(
            "}",
            1
        )[1]

    return tag


def clean_value(value):

    if value is None:
        return ""

    return str(value).strip()


def normalize_key(value):

    if value is None:
        return ""

    value = str(value)

    return "".join(
        c.lower()
        for c in value
        if c.isalnum()
    )


def first_value(
    fields,
    aliases,
    default=""
):

    normalized_fields = {
        normalize_key(k): v
        for k, v in fields.items()
    }

    for alias in aliases:

        key = normalize_key(alias)

        if key in normalized_fields:

            value = normalized_fields[key]

            if value not in (
                None,
                ""
            ):

                return clean_value(value)

    return default


def parse_xml_contexts(xml_path):

    """
    Reads XML and returns:

        main_fields
        disclosure_fields

    disclosure_fields is a list containing
    Disclosure1, Disclosure2, etc.
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

        if context_ref:

            context_lower = (
                context_ref.lower()
            )

            if context_lower == "maini":

                main_fields[tag] = value

            elif context_lower.startswith(
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
# XML FIELD MAPPING
# ============================================================

def convert_disclosure_to_old_format(
    disclosure,
    main_fields,
    filing
):

    """
    Converts the new XML structure into the
    EXACT column structure used by the old scraper.
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

    row["symbol"] = first_value(
        main_fields,
        [
            "Symbol",
            "symbol"
        ],
        filing.get(
            "symbol",
            ""
        )
    )

    row["company"] = first_value(
        main_fields,
        [
            "Company",
            "CompanyName",
            "companyName"
        ],
        filing.get(
            "companyName",
            ""
        )
    )

    row["anex"] = first_value(
        main_fields,
        [
            "DisclosureUnderRegulation",
            "Regulation",
            "regulation"
        ],
        filing.get(
            "regulation",
            ""
        )
    )

    row["xbrl"] = filing.get(
        "ixbrl",
        ""
    )

    row["date"] = first_value(
        main_fields,
        [
            "DateOfFiling",
            "Date",
            "FilingDate"
        ],
        ""
    )

    # ========================================================
    # ACQUIRER / PERSON
    # ========================================================

    row["acqName"] = first_value(
        disclosure,
        [
            "AcquirerName",
            "AcqName",
            "Name",
            "PersonName",
            "NameOfAcquirer",
            "Acquirer"
        ]
    )

    row["pid"] = first_value(
        disclosure,
        [
            "PAN",
            "Pan",
            "ID",
            "Identifier",
            "AcquirerPAN",
            "AcquirerId"
        ]
    )

    row["personCategory"] = first_value(
        disclosure,
        [
            "PersonCategory",
            "Category",
            "AcquirerCategory"
        ]
    )

    # ========================================================
    # SECURITY
    # ========================================================

    row["secType"] = first_value(
        disclosure,
        [
            "SecurityType",
            "SecType",
            "TypeOfSecurity",
            "Security"
        ]
    )

    row["securitiesTypePost"] = first_value(
        disclosure,
        [
            "SecuritiesTypePost",
            "SecurityTypePost",
            "PostSecurityType"
        ]
    )

    # ========================================================
    # TRANSACTION TYPE
    # ========================================================

    transaction_type = first_value(
        disclosure,
        [
            "TransactionType",
            "TdpTransactionType",
            "Transaction",
            "NatureOfTransaction"
        ]
    )

    row["tdpTransactionType"] = (
        transaction_type
    )

    row["secAcq"] = first_value(
        disclosure,
        [
            "SecuritiesAcquiredDisposed",
            "SecAcq",
            "AcquiredDisposed",
            "SecuritiesAcquired",
            "SecuritiesDisposed"
        ]
    )

    # ========================================================
    # HOLDING BEFORE
    # ========================================================

    row["befAcqSharesNo"] = first_value(
        disclosure,
        [
            "HoldingBefore",
            "HoldingBeforeNo",
            "HoldingBeforeShares",
            "BeforeAcquisitionShares",
            "BeforeAcqSharesNo",
            "BefAcqSharesNo",
            "PreTransactionHolding"
        ]
    )

    row["befAcqSharesPer"] = first_value(
        disclosure,
        [
            "HoldingBeforePercentage",
            "HoldingBeforePer",
            "BeforeAcquisitionPercentage",
            "BeforeAcqSharesPer",
            "BefAcqSharesPer",
            "PreTransactionHoldingPercentage"
        ]
    )

    # ========================================================
    # QUANTITY
    # ========================================================

    quantity = first_value(
        disclosure,
        [
            "Quantity",
            "SecuritiesQuantity",
            "NoOfSecurities",
            "AcquiredDisposedQuantity",
            "AcquiredQuantity",
            "DisposedQuantity"
        ]
    )

    # ========================================================
    # VALUE
    # ========================================================

    value = first_value(
        disclosure,
        [
            "Value",
            "TransactionValue",
            "ValueOfTransaction",
            "SecuritiesValue",
            "SecVal"
        ]
    )

    row["secVal"] = value

    # ========================================================
    # BUY / SELL
    # ========================================================

    transaction_lower = (
        transaction_type
        .lower()
        .strip()
    )

    if (
        "buy" in transaction_lower
        or "acquir" in transaction_lower
    ):

        row["buyQuantity"] = quantity
        row["buyValue"] = value

    elif (
        "sell" in transaction_lower
        or "dispos" in transaction_lower
    ):

        row["sellquantity"] = quantity
        row["sellValue"] = value

    else:

        # Some NSE XMLs use a separate
        # acquired/disposed field.

        sec_acq_lower = (
            row["secAcq"]
            .lower()
        )

        if "acquir" in sec_acq_lower:

            row["buyQuantity"] = quantity
            row["buyValue"] = value

        elif "dispos" in sec_acq_lower:

            row["sellquantity"] = quantity
            row["sellValue"] = value

    # ========================================================
    # HOLDING AFTER
    # ========================================================

    row["afterAcqSharesNo"] = first_value(
        disclosure,
        [
            "HoldingAfter",
            "HoldingAfterNo",
            "HoldingAfterShares",
            "AfterAcquisitionShares",
            "AfterAcqSharesNo",
            "PostTransactionHolding"
        ]
    )

    row["afterAcqSharesPer"] = first_value(
        disclosure,
        [
            "HoldingAfterPercentage",
            "HoldingAfterPer",
            "AfterAcquisitionPercentage",
            "AfterAcqSharesPer",
            "PostTransactionHoldingPercentage"
        ]
    )

    # ========================================================
    # TRANSACTION DATE
    # ========================================================

    row["acqfromDt"] = first_value(
        disclosure,
        [
            "TransactionDateFrom",
            "AcquisitionDateFrom",
            "AcqFromDt",
            "FromDate",
            "DateFrom"
        ]
    )

    row["acqtoDt"] = first_value(
        disclosure,
        [
            "TransactionDateTo",
            "AcquisitionDateTo",
            "AcqToDt",
            "ToDate",
            "DateTo"
        ]
    )

    # If XML has one transaction date,
    # populate both fields.

    single_transaction_date = first_value(
        disclosure,
        [
            "TransactionDate",
            "AcquisitionDate",
            "DateOfTransaction"
        ]
    )

    if (
        not row["acqfromDt"]
        and single_transaction_date
    ):

        row["acqfromDt"] = (
            single_transaction_date
        )

    if (
        not row["acqtoDt"]
        and single_transaction_date
    ):

        row["acqtoDt"] = (
            single_transaction_date
        )

    # ========================================================
    # INTIMATION
    # ========================================================

    row["intimDt"] = first_value(
        disclosure,
        [
            "DateOfIntimation",
            "IntimationDate",
            "IntimDt",
            "DateOfIntimationToExchange"
        ]
    )

    # ========================================================
    # MODE
    # ========================================================

    row["acqMode"] = first_value(
        disclosure,
        [
            "ModeOfAcquisition",
            "ModeOfTransaction",
            "AcquisitionMode",
            "AcqMode",
            "Mode"
        ]
    )

    # ========================================================
    # DERIVATIVE
    # ========================================================

    row["derivativeType"] = first_value(
        disclosure,
        [
            "DerivativeType",
            "TypeOfDerivative",
            "Derivative"
        ]
    )

    # ========================================================
    # EXCHANGE
    # ========================================================

    row["exchange"] = first_value(
        disclosure,
        [
            "Exchange",
            "StockExchange"
        ]
    )

    # ========================================================
    # REMARKS
    # ========================================================

    row["remarks"] = first_value(
        disclosure,
        [
            "Remarks",
            "Remark",
            "Comments",
            "RemarksIfAny"
        ],
        filing.get(
            "revisionRemark",
            ""
        ) or ""
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

        # Copy cookies obtained during
        # warm-up.

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

def download_xml(filing):

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

    safe_app_id = (
        app_id
        if app_id
        else "unknown"
    )

    xml_path = os.path.join(
        XML_DIRECTORY,
        f"{safe_app_id}.xml"
    )

    # Already downloaded

    if os.path.exists(
        xml_path
    ):

        return (
            filing,
            xml_path,
            None
        )

    s = get_thread_session()

    headers = get_headers(
        NSE_PAGE
    )

    headers.update({
        "Accept": (
            "application/xml,"
            "text/xml,"
            "*/*"
        )
    })

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

def process_filing(filing):

    filing, xml_path, error = (
        download_xml(filing)
    )

    if error:

        logger.error(
            f"XML failed: "
            f"{filing.get('appId')} "
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
                f"No disclosures found "
                f"in appId={filing.get('appId')}"
            )

        return rows

    except Exception as e:

        logger.exception(
            f"XML parsing failed "
            f"for appId={filing.get('appId')}"
        )

        return []


# ============================================================
# EXACT OLD COLUMN ORDER
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


# ------------------------------------------------------------
# STEP 1
# Get filing metadata
# ------------------------------------------------------------

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


# ------------------------------------------------------------
# STEP 2
# Remove duplicate appIds
# ------------------------------------------------------------

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

        # Keep records without appId
        unique_records[
            f"NO_APP_ID_{len(unique_records)}"
        ] = filing


records = list(
    unique_records.values()
)

print(
    f"Unique filings: {len(records)}"
)


# ------------------------------------------------------------
# STEP 3
# Download + parse XML
# ------------------------------------------------------------

print(
    f"\nDownloading and parsing XML files..."
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
                f"Processing failed "
                f"for appId="
                f"{filing.get('appId')}"
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


# ------------------------------------------------------------
# STEP 4
# Create DataFrame
# ------------------------------------------------------------

df = pd.DataFrame(
    all_rows
)


# ------------------------------------------------------------
# STEP 5
# FORCE EXACT COLUMN STRUCTURE
# ------------------------------------------------------------

for column in OLD_COLUMNS:

    if column not in df.columns:

        df[column] = ""


# This is extremely important.
#
# Even if XML structure changes,
# downstream column names/order remain
# EXACTLY the same.

df = df[
    OLD_COLUMNS
]


# ------------------------------------------------------------
# STEP 6
# Numeric columns
# ------------------------------------------------------------

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


# ------------------------------------------------------------
# STEP 7
# Save CSV
# ------------------------------------------------------------

df.to_csv(
    OUTPUT_CSV,
    index=False,
    encoding="utf-8-sig"
)


# ============================================================
# FINAL SUMMARY
# ============================================================

print()
print("=" * 60)
print("COMPLETED")
print("=" * 60)

print(
    f"Filings found       : {len(records)}"
)

print(
    f"Transactions parsed : {len(df)}"
)

print(
    f"Failed filings      : {failed}"
)

print(
    f"Output              : {OUTPUT_CSV}"
)

print(
    f"XML cache           : {XML_DIRECTORY}"
)

print(
    "=" * 60
)


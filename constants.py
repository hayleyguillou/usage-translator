OUTPUT_FOLDER = "output"
CHARGEABLE_SQL_FILE = "chargeable_insert_rows.sql"
DOMAINS_SQL_FILE = "domains_insert_rows.sql"
DEFAULT_CSV_FILE = "data/Sample_Report.csv"
DEFAULT_JSON_FILE = "data/typemap.json"
DEFAULT_LOG_FILE = "usage_translator.log"

PARTNER_IDS_TO_SKIP = [26392]
UNIT_REDUCTION = {
    "EA000001GB0O": 1000,
    "PMQ00005GB0R": 5000,
    "SSX006NR": 1000,
    "SPQ00001MB0R": 2000,
}

NO_VALID_ROWS_CHARGEABLE_SQL = "-- No valid rows to insert into chargeable table"
NO_VALID_ROWS_DOMAINS_SQL = "-- No valid rows to insert into domains table"

# Column names for the CSV file
PARTNER_ID = "PartnerID"
PART_NUMBER = "PartNumber"
ACCOUNT_GUID = "accountGuid"
PLAN = "plan"
DOMAINS = "domains"
ITEM_COUNT = "itemCount"

REQUIRED_COLUMNS = [PARTNER_ID, PART_NUMBER, ACCOUNT_GUID, PLAN, DOMAINS, ITEM_COUNT]

# SQL insert statement templates
CHARGEABLE_INSERT_HEADER = "INSERT INTO chargeable (partnerID, product, productPurchasedPlanID, plan, usage) VALUES \n"
DOMAINS_INSERT_HEADER = "INSERT INTO domains (domain, partnerPurchasedPlanID) VALUES \n"

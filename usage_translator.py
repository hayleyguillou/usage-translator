import pandas as pd
import json
import argparse
import logging
from pathlib import Path
from constants import OUTPUT_FOLDER, CHARGEABLE_SQL_FILE, DOMAINS_SQL_FILE, DEFAULT_CSV_FILE, DEFAULT_JSON_FILE, DEFAULT_LOG_FILE, PARTNER_IDS_TO_SKIP, UNIT_REDUCTION
from utils import setup_logging, clean_guid
from collections import defaultdict

def generate_chargeable_sql(df, type_map, partner_id_skip_list=PARTNER_IDS_TO_SKIP):
    sql = []
    product_totals = defaultdict(int)
    domain_partners = defaultdict(str)

    for index, row in df.iterrows():
        row_number = index + 2  # Adjust for header row and 0-based index
        
        part_number = row["PartNumber"]
        item_count = row["itemCount"]
        partner_id = row["PartnerID"]
        account_guid = row["accountGuid"]
        domain = row["domains"]
        plan = row["plan"]
        partner_purchased_plan_id = clean_guid(account_guid)

        try:
            if pd.isna(part_number):
                raise ValueError("PartNumber is missing")
            elif not isinstance(item_count, int):
                raise ValueError("ItemCount is not an integer")
            elif item_count <= 0:
                raise ValueError("ItemCount is zero or negative")
            elif partner_id_skip_list and partner_id in partner_id_skip_list:
                raise ValueError(f"PartnerID {partner_id} is in the skip list")
            elif part_number not in type_map:
                raise ValueError(f"PartNumber {part_number} not found in typemap")
            elif len(partner_purchased_plan_id) == 0 or len(partner_purchased_plan_id) > 32:
                raise ValueError(f"Invalid partnerPurchasedPlanID ('{partner_purchased_plan_id}')")
        except ValueError as e:
            logging.warning(f"{e}: skipping row {row_number}")
            continue

        translated_part_number = type_map[part_number]

        if part_number in UNIT_REDUCTION:
            item_count = item_count // UNIT_REDUCTION[part_number]
        
        product_totals[part_number] += item_count
        domain_partners[domain] = partner_purchased_plan_id
        
        sql.append(
            f"INSERT INTO chargeable (partnerID, product, productPurchasedPlanID, plan, usage) "
            f"VALUES ({partner_id}, '{translated_part_number}', '{partner_purchased_plan_id}', '{plan}', {item_count});"
        )
        logging.debug(f"Generated SQL for index {row_number}: {sql[-1]}")        

    return sql, product_totals, domain_partners

def generate_domains_sql(domain_map):
    """
    Generate SQL for domain inserts.

    Assumptions:
    - Each unique domain only has a single corresponding partnerPurchasedPlanID
    - The table is empty before running this script, so no need to check for duplicates
    """
    sql = []
    for domain, partner_purchased_plan_id in domain_map.items():
        sql.append(
            f"INSERT INTO domains (domain, partnerPurchasedPlanID) "
            f"VALUES ('{domain}', '{partner_purchased_plan_id}');"
        )
        logging.debug(f"Generated SQL for domain {domain}: {sql[-1]}")
    return sql

def main():
    parser = argparse.ArgumentParser(description="Usage Translator CLI")
    parser.add_argument("--csv", default=DEFAULT_CSV_FILE, help="Path to the CSV report file")
    parser.add_argument("--json", default=DEFAULT_JSON_FILE, help="Path to the typemap JSON file")
    parser.add_argument("--log", action="store_true", help=f"If set, logs will also be written to {DEFAULT_LOG_FILE}")

    args = parser.parse_args()
    setup_logging(args.log)

    try:
        df = pd.read_csv(args.csv)
        logging.info(f"Loaded CSV: {args.csv}")
    except FileNotFoundError:
        logging.error(f"CSV file not found: {args.csv}")
        return

    try:
        with open(args.json) as f:
            type_map = json.load(f)
            logging.info(f"Loaded JSON: {args.json}")
    except FileNotFoundError:
        logging.error(f"JSON file not found: {args.json}")
        return

    chargeable_sql, item_count_per_part_number, domain_map = generate_chargeable_sql(df, type_map)

    logging.info(f"Item count per part number: {dict(item_count_per_part_number)}")

    domain_sql = generate_domains_sql(domain_map)

    Path(OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)

    with open(f"{OUTPUT_FOLDER}/{CHARGEABLE_SQL_FILE}", "w") as f:
        f.write("\n".join(chargeable_sql) + "\n")
    logging.info(f"Wrote chargeable_inserts.sql with {len(chargeable_sql)} statements")

    with open(f"{OUTPUT_FOLDER}/{DOMAINS_SQL_FILE}", "w") as f:
        f.write("\n".join(domain_sql) + "\n")
    logging.info(f"Wrote domains_inserts.sql with {len(domain_sql)} statements")

if __name__ == "__main__":
    main()

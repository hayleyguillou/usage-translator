import pandas as pd
import json
import argparse
import logging
from constants import OUTPUT_FOLDER, CHARGEABLE_SQL_FILE, DOMAINS_SQL_FILE, DEFAULT_CSV_FILE, DEFAULT_JSON_FILE, DEFAULT_LOG_FILE, REQUIRED_COLUMNS
from utils import setup_logging
from processor import generate_chargeable_sql, generate_domains_sql


def main():
    parser = argparse.ArgumentParser(description="Usage Translator CLI")
    parser.add_argument("--csv", default=DEFAULT_CSV_FILE, help="Path to the CSV report file")
    parser.add_argument("--json", default=DEFAULT_JSON_FILE, help="Path to the typemap JSON file")
    parser.add_argument("--batch-insert-size", default=0, help="Batch insert size for SQL queries")
    parser.add_argument("--log", action="store_true", help=f"If set, logs will also be written to {DEFAULT_LOG_FILE}")

    args = parser.parse_args()
    setup_logging(args.log)

    # Load CSV file into a DataFrame
    try:
        df = pd.read_csv(args.csv)
        logging.info(f"Loaded CSV: {args.csv}")
    except FileNotFoundError:
        logging.error(f"CSV file not found: {args.csv}")
        return
    
    # Check if the CSV is empty
    if df.empty:
        logging.error(f"CSV file is empty: {args.csv}")
        return

    # Check if required columns are present
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        logging.error(f"CSV file is missing required columns: {', '.join(missing_columns)}")
        return

    # Load JSON file for typemap
    try:
        with open(args.json) as f:
            type_map = json.load(f)
            logging.info(f"Loaded JSON: {args.json}")
    except FileNotFoundError:
        logging.error(f"JSON file not found: {args.json}")
        return
    
    with open(f"{OUTPUT_FOLDER}/{CHARGEABLE_SQL_FILE}", "w") as chargeable_sql_output:
        product_totals, domain_map = generate_chargeable_sql(df, type_map, chargeable_sql_output, batch_insert_size=args.batch_insert_size)

    logging.info("Product totals:")
    for part_number, total in product_totals.items():
        logging.info(f"  - Part Number: {part_number}, Total: {total}")
         
    with open(f"{OUTPUT_FOLDER}/{DOMAINS_SQL_FILE}", "w") as domains_sql_output:
        generate_domains_sql(domain_map, domains_sql_output, batch_insert_size=args.batch_insert_size)

if __name__ == "__main__":
    main()

import pandas as pd
import json
import argparse
import logging
from pathlib import Path
from constants import OUTPUT_FOLDER, CHARGEABLE_SQL_FILE, DOMAINS_SQL_FILE, DEFAULT_CSV_FILE, DEFAULT_JSON_FILE, DEFAULT_LOG_FILE
from utils import setup_logging

def generate_chargeable_sql():
    sql = [f"Chargeable insert statement {i}" for i in range(10)]
    return sql

def generate_domains_sql():
    sql = [f"Domain insert statement {i}" for i in range(10)]
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

    chargeable_sql= generate_chargeable_sql()

    domain_sql = generate_domains_sql()

    Path(OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)

    with open(f"{OUTPUT_FOLDER}/{CHARGEABLE_SQL_FILE}", "w") as f:
        f.write("\n".join(chargeable_sql) + "\n")
    logging.info(f"Wrote chargeable_inserts.sql with {len(chargeable_sql)} statements")

    with open(f"{OUTPUT_FOLDER}/{DOMAINS_SQL_FILE}", "w") as f:
        f.write("\n".join(domain_sql) + "\n")
    logging.info(f"Wrote domains_inserts.sql with {len(domain_sql)} statements")

if __name__ == "__main__":
    main()

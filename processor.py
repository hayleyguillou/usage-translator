import pandas as pd
from collections import defaultdict
import logging
from constants import PARTNER_IDS_TO_SKIP, UNIT_REDUCTION, NO_VALID_ROWS_CHARGEABLE_SQL, NO_VALID_ROWS_DOMAINS_SQL
from utils import clean_guid, escape_sql_string

# Constants for SQL generation
CHARGEABLE_INSERT_HEADER = "INSERT INTO chargeable (partnerID, product, productPurchasedPlanID, plan, usage) VALUES \n"
DOMAINS_INSERT_HEADER = "INSERT INTO domains (domain, partnerPurchasedPlanID) VALUES \n"

def generate_chargeable_sql(df, type_map, output_file, partner_id_skip_list=PARTNER_IDS_TO_SKIP, batch_insert_size=0):
    """
    Generate SQL for chargeable inserts.

    Args:
        df (pd.DataFrame): DataFrame containing usage data.
        type_map (dict): Mapping of part numbers to product names.
        output_file: The file to write the SQL insert statements to.
        partner_id_skip_list (list): List of PartnerIDs to skip.
        batch_insert_size (int): The number of rows to include in each batch insert statement.
            default: 0 (no batching).
    """
    product_totals = defaultdict(int)
    domain_partners = defaultdict(str)
    
    rows_to_insert = 0
    batch_count = 0
    insert_started = False

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
            elif not isinstance(partner_id, int):
                raise ValueError("PartnerID is not an integer")
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

        if batch_count == 0:
            output_file.write(CHARGEABLE_INSERT_HEADER)
            insert_started = True
        elif batch_count > 0:
            output_file.write(",\n")

        output_file.write(
            f"\t({partner_id}, '{translated_part_number}', '{partner_purchased_plan_id}', '{escape_sql_string(plan)}', {item_count})"
        )
        rows_to_insert += 1
        batch_count += 1
        logging.debug(f"Processed row {row_number}: {partner_id}, {translated_part_number}, {partner_purchased_plan_id}, {plan}, {item_count}")
        
        if batch_insert_size > 0 and batch_count >= batch_insert_size:
            logging.debug(f"Batch insert size reached: {batch_insert_size}; {rows_to_insert} rows inserted")
            output_file.write(";\n")
            batch_count = 0
            insert_started = False
    
    if rows_to_insert == 0: 
        output_file.truncate(0)
        output_file.write(NO_VALID_ROWS_CHARGEABLE_SQL)
        logging.info("No valid rows to insert into chargeable table")
    elif insert_started:
        output_file.write(";\n")
        logging.info(f"Query inserts {rows_to_insert} rows into chargeable table")

    return product_totals, domain_partners

def generate_domains_sql(domain_map, output_file, batch_insert_size=0):
    """
    Generate SQL query for domain inserts.

    Args:
        domain_map (dict): A dictionary mapping domain names to partnerPurchasedPlanID.
        output_file: The file to write the SQL insert statements to.
        batch_insert_size (int): The number of rows to include in each batch insert statement.
            default: 0 (no batching).

    Returns:
        None
    
    Assumptions:
    - Each unique domain only has a single corresponding partnerPurchasedPlanID
    - The table is empty before running this script, so no need to check for duplicates
    - Domain names shouldn't contain ' characters, but if they do, they will be escaped
    - partnerPurchasedPlanID should be a valid GUID and is already cleaned
    """
    if len(domain_map) == 0:
        output_file.write(NO_VALID_ROWS_DOMAINS_SQL)
        logging.info("No valid rows to insert into domains table")
        return

    rows_to_insert = 0
    batch_count = 0
    insert_started = False

    for domain, partner_purchased_plan_id in domain_map.items():
        if batch_count == 0:
            output_file.write(DOMAINS_INSERT_HEADER)
            insert_started = True
        elif batch_count > 0:
            output_file.write(",\n")
        
        output_file.write(f"\t('{escape_sql_string(domain)}', '{partner_purchased_plan_id}')")
        rows_to_insert += 1
        batch_count += 1
        logging.debug(f"Processed domain {domain}: {partner_purchased_plan_id}")
        
        if batch_insert_size > 0 and batch_count >= batch_insert_size:
            logging.debug(f"Batch insert size reached: {batch_insert_size}; {rows_to_insert} rows inserted")
            output_file.write(";\n")
            batch_count = 0
            insert_started = False

    if insert_started:
        output_file.write(";\n")
        
    logging.info(f"Query inserts {rows_to_insert} rows into domains table")

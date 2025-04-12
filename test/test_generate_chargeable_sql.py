import io
import pandas as pd
from processor import generate_chargeable_sql
from constants import PARTNER_IDS_TO_SKIP, UNIT_REDUCTION, NO_VALID_ROWS_CHARGEABLE_SQL

type_map = {
    "ADS000010U0R": "core.chargeable.adsync",
    "SSX00010GB0R": "core.chargeable.sharesync10gb",
    "CD000001SU0R": "core.chargeable.comdisclaimsvcs",
    "EA000001GB0O": "core.chargeable.addarchiveingestspace",
}

valid_partner_id = 1
partner_id_to_skip = list(PARTNER_IDS_TO_SKIP)[0]

valid_part_number = "ADS000010U0R"
invalid_part_number = "NOT_IN_TYPEMAP"

valid_account_guid = "abc-123"
cleaned_account_guid = "abc123"

unit_reduction_part_number = "EA000001GB0O"

def df_row(partner_id=valid_partner_id, part_number=valid_part_number, account_guid=valid_account_guid, plan="TestPlan", domains="test.example.com", item_count=5):
    """
    Create a DataFrame row with parameters for testing. Defaults to valid values.
    """
    return {
        "PartnerID": partner_id,
        "PartNumber": part_number,
        "accountGuid": account_guid,
        "plan": plan,
        "domains": domains,
        "itemCount": item_count,
    }

def run_generate_chargeable_sql(df, batch_insert_size=0):
    output = io.StringIO()
    product_totals, domain_partners = generate_chargeable_sql(df, type_map, output, batch_insert_size=batch_insert_size)
    output.seek(0)
    return output.read(), product_totals, domain_partners

def run_generate_chargeable_sql_with_logs(caplog, df, log_level="WARNING", batch_insert_size=0):
    """
    Run the generate_chargeable_sql function with logging and capture the output.
    """
    caplog.set_level(log_level)
    output = io.StringIO()
    product_totals, domain_partners = generate_chargeable_sql(df, type_map, output, batch_insert_size=batch_insert_size)
    output.seek(0)
    return output.read(), product_totals, domain_partners

def test_missing_part_number(caplog):
    df = pd.DataFrame([df_row(part_number=None)])
    sql, _, _ = run_generate_chargeable_sql_with_logs(caplog, df)
    assert sql == NO_VALID_ROWS_CHARGEABLE_SQL
    assert "PartNumber is missing: skipping row 2" in caplog.text

def test_zero_negative_item_count(caplog):
    df = pd.DataFrame([df_row(item_count=0), df_row(item_count=-1)])
    sql, _, _ = run_generate_chargeable_sql_with_logs(caplog, df)
    assert sql == NO_VALID_ROWS_CHARGEABLE_SQL
    assert "ItemCount is zero or negative: skipping row 2" in caplog.text
    assert "ItemCount is zero or negative: skipping row 3" in caplog.text

def test_non_numeric_item_count(caplog):
    df = pd.DataFrame([df_row(item_count="not a number")])
    sql, _, _ = run_generate_chargeable_sql_with_logs(caplog, df)
    assert sql == NO_VALID_ROWS_CHARGEABLE_SQL
    assert "ItemCount is not an integer: skipping row 2" in caplog.text

def test_invalid_partner_id(caplog):
    df = pd.DataFrame([df_row(partner_id="not an integer")])
    sql, _, _ = run_generate_chargeable_sql_with_logs(caplog, df)
    assert sql == NO_VALID_ROWS_CHARGEABLE_SQL
    assert "PartnerID is not an integer: skipping row 2" in caplog.text

def test_partner_id_in_list(caplog):
    df = pd.DataFrame([df_row(partner_id=partner_id_to_skip)])
    sql, _, _ = run_generate_chargeable_sql_with_logs(caplog, df, log_level="DEBUG")
    assert sql == NO_VALID_ROWS_CHARGEABLE_SQL
    assert f"PartnerID {partner_id_to_skip} is in the skip list: skipping row 2" in caplog.text

def test_part_number_not_in_typemap(caplog):
    df = pd.DataFrame([df_row(part_number=invalid_part_number)])
    sql, _, _ = run_generate_chargeable_sql_with_logs(caplog, df)
    assert sql == NO_VALID_ROWS_CHARGEABLE_SQL
    assert f"PartNumber {invalid_part_number} not found in typemap: skipping row 2" in caplog.text

def test_invalid_partner_purchased_plan_id(caplog):
    df = pd.DataFrame([
        df_row(account_guid="abc-123-456-789-veryveryveryveryverylongstring"), 
        df_row(account_guid="")
    ])
    sql, _, _ = run_generate_chargeable_sql_with_logs(caplog, df)
    assert sql == NO_VALID_ROWS_CHARGEABLE_SQL
    assert "Invalid partnerPurchasedPlanID" in caplog.text

def test_unit_reduction(caplog):
    item_count = 5000
    reduced_item_count = item_count // UNIT_REDUCTION[unit_reduction_part_number]

    df = pd.DataFrame([
        df_row(part_number=unit_reduction_part_number, item_count=item_count), 
        df_row(item_count=item_count)
    ])

    sql, _, _ = run_generate_chargeable_sql_with_logs(caplog, df)
    assert len(sql.splitlines()) == 3
    assert sql == (
        "INSERT INTO chargeable (partnerID, product, productPurchasedPlanID, plan, usage) VALUES \n"
        f"\t({valid_partner_id}, '{type_map[unit_reduction_part_number]}', '{cleaned_account_guid}', 'TestPlan', {reduced_item_count}),\n"
        f"\t({valid_partner_id}, '{type_map[valid_part_number]}', '{cleaned_account_guid}', 'TestPlan', {item_count});\n"
    )

def test_product_totals():
    df = pd.DataFrame([
        df_row(part_number=valid_part_number, item_count=5),
        df_row(part_number=unit_reduction_part_number, item_count=5000),
        df_row(part_number=valid_part_number, item_count=10)
    ])
    _, product_totals, _ = run_generate_chargeable_sql(df)
    assert len(product_totals) == 2
    assert product_totals[valid_part_number] == 15
    assert product_totals[unit_reduction_part_number] == 5

def test_domain_partners():
    df = pd.DataFrame([
        df_row(domains="test.example.com", account_guid="abc-123"),
        df_row(domains="another.example.com", account_guid="xyz-789")
    ])
    _, _, domain_partners = run_generate_chargeable_sql(df)
    assert domain_partners["test.example.com"] == "abc123"
    assert domain_partners["another.example.com"] == "xyz789"

def test_duplicate_domains_product_pairs():
    df = pd.DataFrame([
        df_row(domains="test.example.com", account_guid="abc-123"),
        df_row(domains="test.example.com", account_guid="xyz-789")
    ])
    _, _, domain_partners = run_generate_chargeable_sql(df)
    assert domain_partners["test.example.com"] == "xyz789"  # Last one wins

def test_valid_data(caplog):
    df = pd.DataFrame([
        df_row(part_number=None),
        df_row(item_count=0),
        df_row(partner_id=partner_id_to_skip),
        df_row()
    ])
    sql, _, _ = run_generate_chargeable_sql_with_logs(caplog, df, log_level="DEBUG")
    assert sql == (
        "INSERT INTO chargeable (partnerID, product, productPurchasedPlanID, plan, usage) VALUES \n"
        f"\t({valid_partner_id}, '{type_map[valid_part_number]}', '{cleaned_account_guid}', 'TestPlan', 5);\n"
    )

def test_batch_insert_size():
    df = pd.DataFrame([
        df_row(part_number=valid_part_number, item_count=5),
        df_row(part_number=unit_reduction_part_number, item_count=5000),
        df_row(part_number=valid_part_number, item_count=10)
    ])
    sql, _, _ = run_generate_chargeable_sql(df, batch_insert_size=2)
    assert len(sql.splitlines()) == 5 # 2 headers and 3 rows
    assert sql == (
        "INSERT INTO chargeable (partnerID, product, productPurchasedPlanID, plan, usage) VALUES \n"
        f"\t({valid_partner_id}, '{type_map[valid_part_number]}', '{cleaned_account_guid}', 'TestPlan', 5),\n"
        f"\t({valid_partner_id}, '{type_map[unit_reduction_part_number]}', '{cleaned_account_guid}', 'TestPlan', 5);\n"
        "INSERT INTO chargeable (partnerID, product, productPurchasedPlanID, plan, usage) VALUES \n"
        f"\t({valid_partner_id}, '{type_map[valid_part_number]}', '{cleaned_account_guid}', 'TestPlan', 10);\n"
    )

def test_sql_injection_with_single_quote():
    df = pd.DataFrame([
        df_row(plan="'); DROP TABLE users; --", item_count=5)
    ])
    sql, _, _ = run_generate_chargeable_sql(df)
    assert len(sql.splitlines()) == 2
    assert sql == (
        "INSERT INTO chargeable (partnerID, product, productPurchasedPlanID, plan, usage) VALUES \n"
        f"\t({valid_partner_id}, '{type_map[valid_part_number]}', '{cleaned_account_guid}', '''); DROP TABLE users; --', 5);\n"
    ) # SQL injection is not executed, but the input is included in the SQL statement

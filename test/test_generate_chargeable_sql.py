import pandas as pd
from usage_translator import generate_chargeable_sql
from constants import PARTNER_IDS_TO_SKIP

type_map = {
    "ADS000010U0R": "core.chargeable.adsync",
    "SSX00010GB0R": "core.chargeable.sharesync10gb",
    "CD000001SU0R": "core.chargeable.comdisclaimsvcs",
}

valid_partner_id = 1
partner_id_to_skip = list(PARTNER_IDS_TO_SKIP)[0]

valid_part_number = "ADS000010U0R"
invalid_part_number = "NOT_IN_TYPEMAP"

valid_account_guid = "abc-123"
cleaned_account_guid = "abc123"

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

def run_with_logs(caplog, df, log_level="WARNING"):
    """
    Run the generate_chargeable_sql function with logging and return the generated SQL.
    """
    caplog.set_level(log_level)
    return generate_chargeable_sql(df, type_map)

def test_missing_part_number(caplog):
    df = pd.DataFrame([df_row(part_number=None)])

    chargeable_sql = run_with_logs(caplog, df)

    assert len(chargeable_sql) == 0
    assert "PartNumber is missing at index 2: skipping row" in caplog.text

def test_zero_negative_item_count(caplog):
    df = pd.DataFrame([df_row(item_count=0), df_row(item_count=-1)])

    chargeable_sql = run_with_logs(caplog, df)

    assert len(chargeable_sql) == 0
    assert "ItemCount is zero or negative at index 2: skipping row" in caplog.text
    assert "ItemCount is zero or negative at index 3: skipping row" in caplog.text

def test_partner_id_in_list(caplog):
    df = pd.DataFrame([df_row(partner_id=partner_id_to_skip)])

    chargeable_sql = run_with_logs(caplog, df, log_level="DEBUG")

    assert len(chargeable_sql) == 0
    assert f"PartnerID {partner_id_to_skip} is in the skip list at index 2: skipping row" in caplog.text

def test_part_number_not_in_typemap(caplog):
    df = pd.DataFrame([df_row(part_number=invalid_part_number)])

    chargeable_sql = run_with_logs(caplog, df)

    assert len(chargeable_sql) == 0
    assert f"PartNumber {invalid_part_number} not found in typemap at index 2: skipping row" in caplog.text

def test_valid_data(caplog):
    df = pd.DataFrame([
        df_row(part_number=None),
        df_row(item_count=0),
        df_row(partner_id=partner_id_to_skip),
        df_row()
    ])

    chargeable_sql = run_with_logs(caplog, df, log_level="DEBUG")

    assert len(chargeable_sql) == 1
    assert chargeable_sql[0] == (
        "INSERT INTO chargeable (partnerID, product, productPurchasedPlanID, plan, usage) "
        f"VALUES ({valid_partner_id}, '{type_map[valid_part_number]}', '{cleaned_account_guid}', 'TestPlan', 'test.example.com', 5);"
    )
    assert "Generated SQL for index 5" in caplog.text

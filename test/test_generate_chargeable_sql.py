import pytest
import pandas as pd
from usage_translator import generate_chargeable_sql
from constants import PARTNER_IDS_TO_SKIP

valid_partner_id = 1
partner_id_to_skip = PARTNER_IDS_TO_SKIP[0]

def df_row(partner_id=valid_partner_id, part_number="ADS000010U0R", account_guid="abc-123", plan="TestPlan", domains="test.example.com", item_count=5):
    return {
        "PartnerID": partner_id,
        "PartNumber": part_number,
        "accountGuid": account_guid,
        "plan": plan,
        "domains": domains,
        "itemCount": item_count
    }

def test_missing_part_number(caplog):
    df = pd.DataFrame([df_row(part_number=None)])

    with caplog.at_level("WARNING"):
        chargeable_sql = generate_chargeable_sql(df)

    assert len(chargeable_sql) == 0
    assert "PartNumber is missing at index 2: skipping row" in caplog.text

def test_zero_negative_item_count(caplog):
    df = pd.DataFrame([df_row(item_count=0), df_row(item_count=-1)])

    with caplog.at_level("WARNING"):
        chargeable_sql = generate_chargeable_sql(df)
    assert len(chargeable_sql) == 0
    assert "ItemCount is zero or negative at index 2: skipping row" in caplog.text
    assert "ItemCount is zero or negative at index 3: skipping row" in caplog.text

def test_partner_id_in_list(caplog):
    df = pd.DataFrame([df_row(partner_id=partner_id_to_skip)])

    with caplog.at_level("DEBUG"):
        chargeable_sql = generate_chargeable_sql(df)

    assert len(chargeable_sql) == 0
    assert f"PartnerID {partner_id_to_skip} is in the skip list at index 2: skipping row" in caplog.text

def test_valid_data(caplog):
    df = pd.DataFrame([
        df_row(part_number=None),
        df_row(item_count=0),
        df_row(partner_id=partner_id_to_skip),
        df_row()
    ])

    with caplog.at_level("DEBUG"):
        chargeable_sql = generate_chargeable_sql(df)

    assert len(chargeable_sql) == 1
    assert chargeable_sql[0] == (
        "INSERT INTO chargeable (PartnerID, PartNumber, accountGuid, plan, domains, itemCount) "
        "VALUES (1, 'ADS000010U0R', 'abc-123', 'TestPlan', 'test.example.com', 5);"
    )
    assert "Generated SQL for index 5" in caplog.text

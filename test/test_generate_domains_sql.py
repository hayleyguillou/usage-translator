from constants import NO_VALID_ROWS_DOMAINS_SQL
from usage_translator import generate_domains_sql

def test_generate_domains_sql():
    domain_map = {
        "example.com": "abc123",
        "test.com": "xyz789"
    }

    expected_sql = (
        "INSERT INTO domains (domain, partnerPurchasedPlanID) VALUES \n"
        "\t('example.com', 'abc123'),\n"
        "\t('test.com', 'xyz789');"
    )

    generated_sql = generate_domains_sql(domain_map)

    assert generated_sql == expected_sql

def test_empty_domain_map():
    domain_map = {}
    expected_sql = NO_VALID_ROWS_DOMAINS_SQL

    generated_sql = generate_domains_sql(domain_map)
    assert generated_sql == expected_sql


import io
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
        "\t('test.com', 'xyz789');\n"
    )

    output = io.StringIO()
    generate_domains_sql(domain_map, output)
    output.seek(0)
    assert output.read() == expected_sql

def test_empty_domain_map():
    domain_map = {}
    output = io.StringIO()
    generate_domains_sql(domain_map, output)
    output.seek(0)
    assert output.read() == NO_VALID_ROWS_DOMAINS_SQL

def test_batch_insert_size():
    domain_map = {
        "example.com": "abc123",
        "test.com": "xyz789",
        "another.com": "def456"
    }

    expected_sql = (
        "INSERT INTO domains (domain, partnerPurchasedPlanID) VALUES \n"
        "\t('example.com', 'abc123'),\n"
        "\t('test.com', 'xyz789');\n"
        "INSERT INTO domains (domain, partnerPurchasedPlanID) VALUES \n"
        "\t('another.com', 'def456');\n"
    )

    output = io.StringIO()
    generate_domains_sql(domain_map, output, batch_insert_size=2)
    output.seek(0)
    assert output.read() == expected_sql

def test_batch_insert_size():
    domain_map = {
        "example.com": "abc123",
        "test.com": "xyz789",
        "another.com": "def456"
    }

    expected_sql = (
        "INSERT INTO domains (domain, partnerPurchasedPlanID) VALUES \n"
        "\t('example.com', 'abc123'),\n"
        "\t('test.com', 'xyz789');\n"
        "INSERT INTO domains (domain, partnerPurchasedPlanID) VALUES \n"
        "\t('another.com', 'def456');\n"
    )

    output = io.StringIO()
    generate_domains_sql(domain_map, output, batch_insert_size=2)
    output.seek(0)
    assert output.read() == expected_sql

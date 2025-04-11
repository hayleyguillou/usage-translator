from usage_translator import generate_domains_sql

def test_generate_domains_sql():
    domain_map = {
        "example.com": "abc123",
        "test.com": "xyz789"
    }

    expected_sql = [
        "INSERT INTO domains (domain, partnerPurchasedPlanID) VALUES ('example.com', 'abc123');",
        "INSERT INTO domains (domain, partnerPurchasedPlanID) VALUES ('test.com', 'xyz789');"
    ]

    generated_sql = generate_domains_sql(domain_map)

    assert generated_sql == expected_sql

def test_empty_domain_map():
    domain_map = {}
    expected_sql = []

    generated_sql = generate_domains_sql(domain_map)
    assert generated_sql == expected_sql


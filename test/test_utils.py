from utils import clean_guid

def test_clean_guid():
    assert clean_guid("799ef0ab-4438-4157-8afc-f6fc4dfe9253") == "799ef0ab443841578afcf6fc4dfe9253"
    assert clean_guid("abc-def-ghi-jkl") == "abcdefghijkl"
    assert clean_guid("123-4567-890") == "1234567890"
    assert clean_guid(1234567890) == "1234567890"
    assert clean_guid("abc123!@#") == "abc123"
    assert clean_guid("!@#$%^&*()") == ""
    assert clean_guid("") == ""
    assert clean_guid(None) == ""
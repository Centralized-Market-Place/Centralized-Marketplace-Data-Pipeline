from price_cleaner import clean_price
def test_clean_price():
    test_cases = [
        ("5000 birr", 5000.0),
        ("Free", None),
        ("50,000", 50000.0),
        ("Negotiable", None),
        ("$99.99", 99.99),
        ("around 3k", 3.0),
        (None, None),
        (3000, 3000.0),
        ("30.5", 30.5),
        ("Contact seller", None),
        ("N/A", None),
        ("Deal", None),
        ("Call for price", None),
        ("1,234.56", 1234.56),
        ("1.234,56", 1.234),  # Only the first number is extracted, ',' ignored
        ("12 345", 12345.0),  # Spaces removed, becomes 12345
        ("USD 1,000", 1000.0),
        ("ETB 2,500.00", 2500.0),
        ("Not specified", None),
        ("0", 0.0),
        ("-1000", 1000.0),    # '-' is not included in regex, so '1000'
        ("1.000", 1000.0),    # Matches pattern, '.' removed
        ("1,000.00", 1000.0),
        ("1000.00", 1000.0),
        ("1,000,000", 1000000.0),
        ("1.000.000", 1000000.0),  # '.' removed
        ("1000", 1000.0),
        ("1 000", 1000.0),    # Spaces removed, becomes 1000
        ("Price: 2500", 2500.0),
        ("Birr 3,500", 3500.0),
        ("Approximately 4,000", 4000.0),
        ("about 2000", 2000.0),
        ("two thousand", None),
        ("5k", 5.0),          # Only the number is extracted, 'k' ignored
        ("3.5k", 3.5),        # Only the number is extracted, 'k' ignored
        ("1,2,3,4", 1234.0),  # All commas removed, becomes 1234
        ("1.2.3.4", 1234.0),  # All dots removed, becomes 1234
        ("999,999.99", 999999.99),
        ("999.999,99", 999.999), # Only the first number is extracted, ',' ignored
        ("Contact", None),
        ("negotiable", None),
        ("free", None),
        ("N/a", None),
        ("Deal", None),
        ("call", None),
        ("deal", None),
        ("N/A", None),
        ("n/a", None),
        ("Contact Seller", None),
        ("contact seller", None),
        ("Price upon request", None),
        ("upon request", None),
    ]

    for inp, expected in test_cases:
        result = clean_price(inp)
        assert (result == expected or (result is None and expected is None)), f"Failed for input: {inp!r}, got {result}, expected {expected}"

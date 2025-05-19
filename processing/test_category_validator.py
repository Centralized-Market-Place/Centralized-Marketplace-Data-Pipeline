from category_validator import validate_and_clean_categories

def test_validate_and_clean_categories():
    # Valid top-level and one subcategory
    assert validate_and_clean_categories(["technology", "phones"]) == ["technology", "phones"]
    # Valid top-level and multiple valid subcategories
    assert validate_and_clean_categories(["clothes", "men", "shirts", "jeans"]) == ["clothes", "men", "shirts", "jeans"]
    # Valid top-level, some invalid subcategories
    assert validate_and_clean_categories(["shoes", "sneakers", "flipflops", "boots"]) == ["shoes", "sneakers", "boots"]
    # Valid top-level, all invalid subcategories
    assert validate_and_clean_categories(["accessories", "hats", "scarves"]) == ["accessories"]
    # Invalid top-level
    assert validate_and_clean_categories(["food", "pizza"]) == []
    # Not a list
    assert validate_and_clean_categories("technology") == []
    # List with non-string
    assert validate_and_clean_categories(["technology", 123]) == []
    # Valid top-level, no subcategory
    assert validate_and_clean_categories(["shoes"]) == ["shoes"]
    # Valid top-level, valid subcategory with spaces and case
    assert validate_and_clean_categories(["  Technology ", " Phones "]) == ["technology", "phones"]
    assert validate_and_clean_categories(["Clothes", "Evening Dresses"]) == ["clothes", "evening dresses"]
    # Empty list
    assert validate_and_clean_categories([]) == []
    # Valid top-level, repeated subcategories
    assert validate_and_clean_categories(["technology", "phones", "phones", "laptops"]) == ["technology", "phones", "phones", "laptops"]
    # Valid top-level, subcategory with extra spaces
    assert validate_and_clean_categories(["accessories", "  watches  "]) == ["accessories", "watches"]

# test_validate_and_clean_categories()
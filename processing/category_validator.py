# from ..ingestion.constants import VALID_CATEGORIES

VALID_CATEGORIES = {
        "technology": {"phones", "smartphones", "android", "iphone", "laptops", "gaming", "tablets", "wearables"},
        "clothes": {"men", "women", "dresses", "shirts", "t-shirts", "jeans", "evening dresses"},
        "shoes": {"men", "women", "formal", "casual", "oxford", "sneakers", "boots"},
        "accessories": {"bags", "backpacks", "laptop bags", "jewelry", "watches", "belts"},
    }


def validate_and_clean_categories(category_list):
    
    if not category_list or not isinstance(category_list, list):
        return []
    
    if not all(isinstance(c, str) for c in category_list):
        return []

    # Normalize: lowercase and strip
    normalized = [c.strip().lower() for c in category_list]

    # Validate top-level
    top_level = normalized[0]
    if top_level not in VALID_CATEGORIES:
        print(f"❌ Invalid top-level category: '{top_level}'")
        return []

    valid_subs = VALID_CATEGORIES[top_level]
    cleaned = [top_level]

    # Add only valid subcategories
    for sub in normalized[1:]:
        if sub in valid_subs:
            cleaned.append(sub)
        else:
            print(f"⚠️ Skipped invalid subcategory: '{sub}'")

    return cleaned



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
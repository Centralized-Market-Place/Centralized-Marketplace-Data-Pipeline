# from ..ingestion.constants import VALID_CATEGORIES

VALID_CATEGORIES = {
    "technology": {"phones", "smartphones", "android", "iphone", "laptops", "gaming", "tablets", "wearables"},
    "clothes": {"men", "women", "dresses", "shirts", "t-shirts", "jeans", "evening dresses"},
    "shoes": {"men", "women", "formal", "casual", "oxford", "sneakers", "boots"},
    "accessories": {"bags", "backpacks", "laptop bags", "jewelry", "watches", "belts"},
}


def validate_and_clean_categories(category_list):
    try:
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
    except Exception as e:
        print(f"❌ Error trying to validate category: {category_list}. Error: {e}")
        return []


def ensure_string(text):
    """
    Ensures the input is a string. If it's a list of strings, join them by comma.
    If it's another type, convert to string if possible.
    Returns an empty string for None or unconvertible input.
    """
    if isinstance(text, str):
        return text
    elif isinstance(text, list) and all(isinstance(item, str) for item in text):
        return ", ".join(text)
    elif text is None:
        return ""
    try:
        return str(text)
    except Exception:
        return ""
    
def ensure_list(text):
    """
    Ensures the input is a list of strings.
    If it's a single string, returns [text].
    If it's a list of strings, returns as is.
    If it's None, returns an empty list.
    Otherwise, tries to convert to string and wrap in a list.
    """
    if not text: # "", [], None
        return []
    if isinstance(text, list) and all(isinstance(item, str) for item in text):
        return text
    if isinstance(text, str):
        return [text]
    try:
        return [str(text)]
    except Exception:
        return []



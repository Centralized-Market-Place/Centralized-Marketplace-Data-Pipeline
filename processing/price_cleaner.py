import re

def clean_price(raw_price):
    """
    Attempts to convert a price value to a float.
    Returns None if conversion is not possible or price is not numeric.
    """
    if not raw_price:
        return None

    if isinstance(raw_price, (int, float)):
        return float(raw_price)

    # Convert to lowercase string
    price_str = str(raw_price).lower().strip()

    # Remove common non-numeric words
    non_numeric_keywords = ["negotiable", "free", "contact", "n/a", "call", "deal"]
    if any(word in price_str for word in non_numeric_keywords):
        return None

    # Extract number using regex
    match = re.search(r"[\d.,\s]+", price_str)
    if not match:
        return None

    number = match.group(0).replace(" ", "")  # Remove spaces
    if re.match(r"^\d+\.\d{3}$", number):
        number = number.replace(".", "")
    else:
        number = number.replace(",", "")

    try:
        return float(number)
    except ValueError:
        return None



def sanitize_price(price, desc):
    """
    Cleans and sanitizes a price value.
    Returns a float if valid, None otherwise.
    """
    yes = False
    price = clean_price(price)
    if price is None:
        return None
    
    try:
        for i in range(10, 0, -1):
            pr = str(price) + ('0'*i)
            if pr in desc:
                price = float(pr.replace('.', ''))
                yes = True
                break
        
        if not yes:
            pr = str(price)
            if pr + 'k' in desc or pr + 'K' in desc:
                price = float(pr.replace('.', '')) * 1000  
        return price
    except Exception as e:
        print(f"❌ Error sanitizing price '{price}' {e}")
        return None
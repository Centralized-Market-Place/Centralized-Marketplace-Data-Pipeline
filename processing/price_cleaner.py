import requests
import json
import datetime
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
    match = re.search(r"[\d.,]+", price_str)
    if not match:
        return None

    # Clean number (handle comma vs dot formats)
    number = match.group(0).replace(",", "")
    try:
        return float(number)
    except ValueError:
        return None


def price_cleaner_test():
    examples = [
        "5000 birr", "Free", "50,000", "Negotiable", "$99.99", "around 3k", None, 3000, "30.5", "Contact seller"
    ]

    for e in examples:
        print(f"{e!r} ➜ {clean_price(e)}")


def fetch_weekly_rates(base_currencies=["USD", "EUR", "GBP"], target="ETB"):
    url = "https://api.exchangerate.host/latest"
    rates = {}

    for base in base_currencies:
        params = {"base": base, "symbols": target}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            rate = data["rates"].get(target)
            if rate:
                rates[base.lower()] = rate
        except Exception as e:
            print(f"Failed to fetch rate for {base}: {e}")

    # Add default ETB rate for itself
    rates["etb"] = 1.0

    # Save to local file
    with open("etb_rates.json", "w") as f:
        json.dump({
            "fetched_at": datetime.datetime.utcnow().isoformat(),
            "rates": rates
        }, f, indent=2)

    return rates


def load_exchange_rates():
    try:
        with open("etb_rates.json", "r") as f:
            return json.load(f)["rates"]
    except FileNotFoundError:
        return fetch_weekly_rates()  # fallback fetch


def clean_price_to_etb(raw_price, rates):
    if not raw_price:
        return None

    if isinstance(raw_price, (int, float)):
        return float(raw_price)

    price_str = str(raw_price).lower().strip()

    non_numeric_keywords = ["free", "negotiable", "contact", "call", "deal", "n/a"]
    if any(word in price_str for word in non_numeric_keywords):
        return None

    # Determine currency
    currency_map = {
        "$": "usd",
        "usd": "usd",
        "€": "eur",
        "eur": "eur",
        "£": "gbp",
        "gbp": "gbp",
        "birr": "etb",
        "etb": "etb"
    }

    currency = "etb"
    rate = 1.0
    for symbol, curr in currency_map.items():
        if symbol in price_str:
            currency = curr
            rate = rates.get(curr, 1.0)
            break

    match = re.search(r"[\d,.]+", price_str)
    if not match:
        return None

    number_str = match.group(0).replace(",", "")
    try:
        value = float(number_str)
        return round(value * rate, 2)
    except ValueError:
        return None

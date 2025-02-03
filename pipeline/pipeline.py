import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingestion.fetcher import fetch_messages, fetch_from_new_channels
from storage.store import store_raw_data, fetch_stored_messages
from processing.preprocessor import preprocess, extract_oldest_and_latest_ids, insert_into_products_collection

def run_pipeline():
    # messages = fetch_messages()
    fetch_from_new_channels()
    # preprocess()
    # insert_into_products_collection()

if __name__ == "__main__":
    run_pipeline()

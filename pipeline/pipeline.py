import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingestion.fetcher import fetch_messages
# from processing.extractor import extract_features
from storage.store import store_raw_data, fetch_stored_messages
from processing.preprocessor import preprocess

def run_pipeline():
    # messages = fetch_messages()
    preprocess()
    print("Messages fetched successfully.")

    print("Pipeline executed successfully.")


if __name__ == "__main__":
    run_pipeline()

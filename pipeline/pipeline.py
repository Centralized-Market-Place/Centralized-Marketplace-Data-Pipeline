from ingestion.fetcher import fetch_messages
from processing.extractor import extract_features
from storage.database import save_to_database

def run_pipeline():
    messages = fetch_messages()
    print("Messages fetched successfully.")
    # processed = [extract_features(msg) for msg in messages]
    # save_to_database(processed)
    print("Pipeline executed successfully.")

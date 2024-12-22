import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingestion.fetcher import fetch_messages
# from processing.extractor import extract_features
# from storage.database import save_to_database

def run_pipeline():
    messages = fetch_messages()
    
    print("Messages fetched successfully.")
    print(messages[:5])
    # processed = [extract_features(msg) for msg in messages]
    # save_to_database(processed)
    print("Pipeline executed successfully.")


if __name__ == "__main__":
    run_pipeline()

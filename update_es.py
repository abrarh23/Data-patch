from elasticsearch import Elasticsearch
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()
from elasticsearch import Elasticsearch, helpers
from datetime import datetime, timezone
import time

# Load environment variables

ELASTIC_PROD_INDEX_NAME = os.getenv("ELASTIC_PROD_INDEX_NAME")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")
ELASTIC_USERNAME = os.getenv("ELASTIC_USERNAME")
ELASTIC_URL = os.getenv("ELASTIC_URL")

def connect_to_es() -> Elasticsearch:
    # Set up the Elasticsearch client
    es = Elasticsearch(
        [ELASTIC_URL],
        basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
        verify_certs=True
    )
    # Check if the connection was successful
    if es.ping():
        print(f"Connected to {ELASTIC_PROD_INDEX_NAME}")
    else:
        print("Could not connect to Elasticsearch")
    return es

def read_csv_file(csv_file_path: str) -> pd.DataFrame:
    # Read updates from CSV
    updates_df = pd.read_csv(csv_file_path, encoding='utf-8', low_memory=False)
    test_df = updates_df.copy()
    return test_df

def preprocessing_data(test_df: pd.DataFrame) -> pd.DataFrame:
    results_data = test_df.copy()
    results_data = results_data[['_id', 'nationality_country']]
    nationality_df = pd.read_json(r".\nationalities.json", encoding='utf-8')
    final_df = pd.merge(left=results_data, right=nationality_df, how='left', left_on='nationality_country', right_on='nationality')[['_id', 'nationality_country', 'nationality', 'alpha_2_code']]
    return final_df

def execute_bulk_ops(es: Elasticsearch, final_df: pd.DataFrame):
    # Define the index name
    index_name = ELASTIC_PROD_INDEX_NAME

    # Define the chunk size
    chunk_size = 100  # Adjust chunk size as needed

    # Iterate over the DataFrame in chunks
    for chunk_start in range(0, len(final_df), chunk_size):
        chunk_end = min(chunk_start + chunk_size, len(final_df))
        chunk = final_df.iloc[chunk_start:chunk_end]
        
        # Prepare bulk actions list for the current chunk
        bulk_actions = []

        for idx, row in chunk.iterrows():
            # Define the search query to find the document
            query = {
                "query": {
                    "term": {
                        "_id": f"{row['_id']}"
                    }
                }
            }

            # Search for the document to get the document ID
            response = es.search(index=index_name, body=query)
            hits = response['hits']['hits']
            print("Idx:", idx)

            if len(hits) > 0:
                document_id = hits[0]['_id']  # Get the document ID
                existing_nationality = hits[0]['_source'].get('nationality', None)
                document_identifier = hits[0]['_source'].get('identifier', None)

                if document_id == document_identifier:
                    print("Ids matched:", document_id)
                    print("Existing nationality value in ES:", existing_nationality)
                    print("New nationality value will be:", str(row['alpha_2_code']) if str(row['alpha_2_code']) != 'nan' else None)

                    # Prepare the bulk action for update
                    bulk_actions.append({
                        "_op_type": "update",
                        "_index": index_name,
                        "_id": document_id,
                        "doc": {
                            "nationality": str(row['alpha_2_code']) if str(row['alpha_2_code']) != 'nan' else None,  
                            "updatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" # Set updatedAt to current UTC time
                        }
                    })
                else:
                    print("Ids don't match")
            else:
                print(f"No document found for candidate_id: {row['_id']}")

        # Perform bulk update operation for the current chunk
        if bulk_actions:
            helpers.bulk(es, bulk_actions)
            print(f"Bulk update operation completed for chunk {chunk_start} - {chunk_end}. Actual bulk operations count:", len(bulk_actions))
        else:
            print(f"No actions to update for chunk {chunk_start} - {chunk_end}.")

def main():
    es = connect_to_es()
    test_df = read_csv_file(r'.\data\non_emiratis.csv')
    final_df = preprocessing_data(test_df)
    print("After preprocessing data running on:", len(final_df), "candidates")
    # time.sleep(10)
    execute_bulk_ops(es, final_df)

if __name__ == "__main__":
    main()

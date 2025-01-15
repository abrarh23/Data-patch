import pandas as pd
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
import os
from bson.objectid import ObjectId
load_dotenv()
from datetime import datetime, timezone
import time

def connect_mongodb():
    # Connect to MongoDB
    MONGODB_PROD_URI = os.getenv("MONGODB_PROD_URI")
    client = MongoClient(MONGODB_PROD_URI)  # Update with your connection string
    # If connection is established 
    if client:
        print("Connected to:", MONGODB_PROD_URI)
    db = client['qureos-v3']  # Replace with your database name
    collection_apprentices_users = db['apprentices']  # Replace with your collection name
    return collection_apprentices_users

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

def prepare_bulk_ops(final_df: pd.DataFrame):
    operations_apprentices_users = []
    current_timestamp = datetime.now(timezone.utc)  # Get the current UTC timestamp
    valid_bson_count = 0
    for _, row in final_df.iterrows():
        if ObjectId.is_valid(row['_id']):
            valid_bson_count += 1
            update_operation = {
                '$set': {
                    'nationality': str(row['nationality']) if str(row['nationality']) != 'nan' else None,
                    'updatedAt': current_timestamp  # Include the updatedAt field
                }
            }
                        
            operations_apprentices_users.append(
                UpdateOne(
                    {'_id': ObjectId(row['_id'])}, 
                    update_operation
                )
            )
            print("Idx:", valid_bson_count, "\nCandidate Id:", row['_id'], "\nold_nationality: Emirati", "\nnew_nationality:", str(row['nationality']) if str(row['nationality']) != 'nan' else None)
            print(update_operation)
        else:
            print(f"Skipping invalid ObjectId: {row['_id']}")

    return operations_apprentices_users

def execute_bulk_ops(collection_apprentices_users, operations_apprentices_users: list, batch_size: int):
    # Execute bulk update in batches
    if operations_apprentices_users:
        for i in range(0, len(operations_apprentices_users), batch_size):
            apprentices_users_batch = operations_apprentices_users[i:i + batch_size]
            result = collection_apprentices_users.bulk_write(apprentices_users_batch)
            print(f'Modified count for external users batch starting at index {i}: {result.modified_count}')

if __name__ == "__main__":
    collection_apprentices_users = connect_mongodb()
    test_df = read_csv_file(r'.\data\non_emiratis.csv')
    final_df = preprocessing_data(test_df)
    print("Running on:", len(final_df), "candidates")
    # time.sleep(10)
    operations_apprentices_users = prepare_bulk_ops(final_df)
    execute_bulk_ops(collection_apprentices_users,  
                     operations_apprentices_users, 
                     batch_size=100)


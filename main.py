from google.cloud import bigquery

# Set up your Google Cloud project ID and BigQuery dataset and table information
project_id = "kylash-edw"
dataset_id = "dbt_kna"
table_id = "test_inserts"

# Create a BigQuery client
client = bigquery.Client()

import os

# Printing all environment variables
for key, value in os.environ.items():
    print(f'{key}======>>>>>>>>>: {value}')

print("ENV key for the GCP service account is")
print(os.environ.get('GOOGLE_GHA_CREDS_PATH'))


print("STARTING THE SCRIPT")
# Define the rows to be inserted
rows_to_insert = [
    {"name": "Alice", "age": 30, "gender": "Female"},
    {"name": "Bob", "age": 25, "gender": "Male"},
    {"name": "Charlie", "age": 35, "gender": "Male"}
]

# Get the BigQuery table
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)

# Insert the rows into the BigQuery table
errors = client.insert_rows(table, rows_to_insert)

if errors == []:
    print("Records inserted successfully.")
else:
    print("Errors occurred while inserting records:", errors)

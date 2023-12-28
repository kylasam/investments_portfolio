from google.cloud import bigquery

def print_current_timestamp():
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Construct the SQL query to get the current timestamp
    query = "SELECT CURRENT_TIMESTAMP() as current_time"

    # Execute the query
    query_job = client.query(query)

    # Get the results
    results = query_job.result()

    # Print the current timestamp
    for row in results:
        print("Current Timestamp from BigQuery:", row['current_time'])

# Call the function to print the current timestamp
print_current_timestamp()



# from google.cloud import bigquery
# from google.cloud import bigquery
# from google.oauth2 import service_account
# import os
# # Set up your Google Cloud project ID and BigQuery dataset and table information
# project_id = "kylash-edw"
# dataset_id = "dbt_kna"
# table_id = "test_inserts"

# # Create a BigQuery client

# import google.auth

# credentials, project = google.auth.default()
# # credentials = service_account.Credentials.from_service_account_file(os.environ.get('GOOGLE_GHA_CREDS_PATH'))
# print("STARTING THE SCRIPT")
# client = bigquery.Client()
# print("GOT BQ CLIENT",client)




# # Printing all environment variables
# for key, value in os.environ.items():
#     print(f'{key}======>>>>>>>>>: {value}')

# print("ENV key for the GCP service account is")
# print(os.environ.get('GOOGLE_GHA_CREDS_PATH'))

# print("SPREPRE ROWS")

# # Define the rows to be inserted
# rows_to_insert = [
#     {"name": "Alice", "age": 30, "gender": "Female"},
#     {"name": "Bob", "age": 25, "gender": "Male"},
#     {"name": "Charlie", "age": 35, "gender": "Male"}
# ]

# # Get the BigQuery table
# table_ref = client.dataset(dataset_id).table(table_id)
# table = client.get_table(table_ref)

# print("INSERT ROWS")
# # Insert the rows into the BigQuery table
# errors = client.insert_rows(table, rows_to_insert)

# if errors == []:
#     print("Records inserted successfully.")
# else:
#     print("Errors occurred while inserting records:", errors)

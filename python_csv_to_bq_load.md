### Python Script to Load CSV Data from GCS to BigQuery (Append Mode) - when the table is already existing.
#### Use this when the CSV file column and the table columns are in the same order. 
```python
from google.cloud import bigquery

# Define your variables
gcs_bucket_name = "your-gcs-bucket-name"  # e.g., "my-bucket"
csv_file_name = "your-file.csv"  # e.g., "data/myfile.csv"
project_id = "your-project-id"  # e.g., "my-gcp-project"
dataset_id = "your-dataset-id"  # e.g., "my_dataset"
table_id = "your-table-id"  # e.g., "my_table"

# Initialize BigQuery client
bq_client = bigquery.Client(project=project_id)

# Define the GCS URI (path to your CSV file in GCS)
gcs_uri = f"gs://{gcs_bucket_name}/{csv_file_name}"

# Define the table reference (this is where the data will be appended)
table_ref = bq_client.dataset(dataset_id).table(table_id)

# Configure the load job settings
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,  # Data source is CSV format
    skip_leading_rows=1,  # Skips header row in the CSV, if applicable
    autodetect=True,  # Automatically detect schema
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append data to existing table
)

# Load data from GCS to BigQuery
load_job = bq_client.load_table_from_uri(
    gcs_uri,
    table_ref,
    job_config=job_config
)

# Wait for the job to complete
load_job.result()

# Get details of the destination table to verify the number of rows loaded
table = bq_client.get_table(table_ref)
print(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}.")
```

- Schema Definition: If the schema of the CSV file is known, you can explicitly define the schema instead of using autodetect. For example:
```
schema = [
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("age", "INTEGER"),
    bigquery.SchemaField("email", "STRING"),
]
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    schema=schema,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
)
```

### Load Data Using Column Mapping (Via a Temporary Table)
#### Use this when the CSV file column and the table columns are NOT in the same order. 

```python
from google.cloud import bigquery

## Step 1: Step 1: Load Data into a Temporary Table. 

# Initialize the BigQuery client
bq_client = bigquery.Client()

# Set variables
gcs_uri = "gs://your-gcs-bucket/your-csv-file.csv"
temp_table_id = "your_project.your_dataset.temp_table"  # Temporary table to hold CSV data
existing_table_id = "your_project.your_dataset.existing_table"  # Existing BQ table

# Define the schema based on the CSV file column order
temp_schema = [
    bigquery.SchemaField("email", "STRING"),  # Assuming email is first in the CSV
    bigquery.SchemaField("age", "INTEGER"),   # Assuming age is second in the CSV
    bigquery.SchemaField("name", "STRING"),   # Assuming name is third in the CSV
]

# Create the load job config
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # Skip header row
    schema=temp_schema,  # Schema matching CSV file column order
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite the temp table
)

# Load the data into the temporary table
load_job = bq_client.load_table_from_uri(
    gcs_uri,
    temp_table_id,
    job_config=job_config
)
load_job.result()  # Wait for the job to complete

print(f"Loaded data into {temp_table_id}.")



## Step 2: Use SQL Query to Load Data into the Existing Table

# Write a SQL query to map columns from the temp table to the existing table
query = f"""
    INSERT INTO `{existing_table_id}` (name, age, email)
    SELECT name, age, email
    FROM `{temp_table_id}`
"""

# Execute the query
query_job = bq_client.query(query)
query_job.result()  # Wait for the job to complete

print(f"Appended data to {existing_table_id} from {temp_table_id}.")


## Step 3 : delete the temp table. 

# Clean up: delete the "temporary" table after use
bq_client.delete_table(temp_table_id)  # Delete the temporary table
print(f"Temporary table {temp_table_id} deleted.")
```

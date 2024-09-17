###### PYTHON ###### LOAD FILE TO BQ Table !!!!
## Load data from GCS bucket CSV file to BQ table, create table if not exists.
## Can use this script in GCP dataproc cluster. 

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage

# Define your GCS and BigQuery configurations
bucket_name = 'gs://abc-10505826-hubhkds-ah-wpb-dna-esg'
excel_file_path = 'EV_AH_Summary_View.csv'
dataset_name = 'DS_AH_WPB_DA_EG_PROD'
table_name = 'AH_EV_Summary_Demo'

# Function to load Excel data into pandas DataFrame
def load_excel_from_gcs(bucket_name, file_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    with blob.open('rb') as f:
        df = pd.read_csv('gs://abc-10505826-hubhkds-amh-wpb-dna-esg/EV_AH_Summary_View.csv')
    return df
	
# Function to upload DataFrame to BigQuery
def upload_to_bigquery(df, dataset_name, table_name):
    bq_client = bigquery.Client()
    dataset_ref = bq_client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
	
# Check if table exists, create if not
    try:
        bq_client.get_table(table_ref)
    except Exception as e:
        print(f"Table {dataset_name}.{table_name} does not exist. Creating it now...")
        table = bigquery.Table(table_ref)
        table = bq_client.create_table(table)
        print(f"Created table {dataset_name}.{table_name}")

    # Load data into BigQuery table
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True  # Automatically detect schema
    job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
	
# Main function to orchestrate the process
def main():
    # Load Excel data into pandas DataFrame
    df = load_excel_from_gcs(bucket_name, excel_file_path)
    
    # Process data if needed (e.g., data cleaning, transformation)
    # Example: Convert date columns to datetime format
    # df['date_column'] = pd.to_datetime(df['date_column'], format='%Y-%m-%d')
    
    # Upload processed data to BigQuery (creates table if not exists)
    upload_to_bigquery(df, dataset_name, table_name)
    print(f'Data uploaded to BigQuery table {dataset_name}.{table_name} successfully.')

if __name__ == '__main__':
    main()

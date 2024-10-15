### Load CSV file from GCP bucket to BQ table. Auto create temp table first, then load the final table from temp table. Finally delete the temp table created. 

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from google.cloud import bigquery

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Load CSV from GCS to BigQuery and Additional Operations") \
    .getOrCreate()

# Define the schema you want for the BigQuery table
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True)
])

# Path to the CSV file in GCS bucket
gcs_path = "gs://your-bucket-name/path/to/your-file.csv"

# Read the CSV file from GCS into a PySpark DataFrame, using the predefined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(gcs_path)

# Show the DataFrame (for debugging purposes)
df.show()

# BigQuery intermediate table (temporary table created from CSV)
bq_temp_table = "your-project-id.your_dataset.your_temp_table_name"

# Write the DataFrame to BigQuery (temporary table)
df.write \
    .format("bigquery") \
    .option("table", bq_temp_table) \
    .option("writeMethod", "direct") \
    .mode("overwrite") \
    .save()

# Step 1: Load data from the first (temporary) table and write to a final table with only selected columns
# Select specific columns
df_final = spark.read \
    .format("bigquery") \
    .option("table", bq_temp_table) \
    .load() \
    .select("id", "name", "email")  # Selecting only specific columns for the final table

# Final BigQuery table (desired final table)
bq_final_table = "your-project-id.your_dataset.your_final_table_name"

# Write the DataFrame with selected columns to the final BigQuery table.
df_final.write \
    .format("bigquery") \
    .option("table", bq_final_table) \
    .option("writeMethod", "direct") \
    .mode("overwrite") \ # Use 'append' if you want to add to an existing table.
    .save()

# Step 2: Delete the intermediate table (temporary table) from BigQuery after loading the final table
# Initialize BigQuery Client
client = bigquery.Client()

# Construct a reference to the temporary table
temp_table_ref = client.dataset("your_dataset").table("your_temp_table_name")

# Delete the temporary table
client.delete_table(temp_table_ref)  # API request
print(f"Table {bq_temp_table} deleted.")

# Stop the SparkSession
spark.stop()
```

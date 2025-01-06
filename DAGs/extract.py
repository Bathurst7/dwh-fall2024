import logging
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from fastavro import writer, parse_schema
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook

# Set up logging
logger = logging.getLogger('airflow.task')

pg_conn = 'cloudsql_postgres_conn'
gcp_conn = 'google_cloud_default'

# Define connection parameters for PostgreSQL connection
def get_postgresql_connection():
    conn = BaseHook.get_connection(pg_conn)
    user_name = conn.login
    password = conn.password
    ip = conn.host
    database_name = conn.schema
    connection_string = f"postgresql+psycopg2://{user_name}:{password}@{ip}:5432/{database_name}"
    return create_engine(connection_string)  # Return the connection engine

# Fetch table names from PostgreSQL database
def fetch_table_names():
    engine = get_postgresql_connection()  # Get the connection engine
    with engine.connect() as conn:
        query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"
        table_names = pd.read_sql(query, conn)  # Use the connection to execute the query
    return table_names['table_name'].tolist()  # Return table names as a list

# Function to check if a column is datetime-like
def is_datetime_column(df, col):
    non_null_values = df[col].dropna()  # Ignore nulls during the check
    if non_null_values.empty:  # If all values are null, it can't be datetime
        return False

    # Attempt to convert the non-null values to datetime
    try:
        pd.to_datetime(non_null_values, errors='raise')  # Raise error for invalid dates
        return True  # If conversion succeeds, it's a datetime column
    except (ValueError, TypeError):
        return False

# Function to process the DataFrame and handle datetimes
def process_dataframe(df, original_types):
    for col in df.columns:
        if original_types[col] == 'object':
            if is_datetime_column(df, col):
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S')
                df[col] = df[col].where(df[col].notnull(), None)
            else:
                df[col] = df[col].astype(str).replace({'nan': None, 'None': None, '': None})
        elif original_types[col] == 'datetime64[ns]':
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
            df[col] = df[col].where(pd.notnull(df[col]), None)

def get_avro_schema(df, data_type_to_avro, table_name, namespace=None, doc=None):
    schema = {
        "name": table_name,        # The name of the record
        "type": "record",          # Type of Avro schema: 'record'
        "fields": []               # List of fields
    }

    # Add fields based on the DataFrame columns and their types
    for col in df.columns:
        dtype = df[col].dtype
        avro_type = data_type_to_avro.get(str(dtype), 'string')  # Default to 'string' for unknown types

        # Check if the column has null values
        if df[col].isnull().any():
            avro_type = ["null", avro_type]  # Allow null in the field

        # Check for datetime64 and handle accordingly
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            avro_type = ["null", "string"]  # Use string for datetime (ISO 8601 format)

        # Special case: handle boolean columns explicitly
        elif pd.api.types.is_bool_dtype(df[col]):
            avro_type = ["null", "boolean"]  # Allow null and map to Avro boolean

        schema['fields'].append({
            "name": col,
            "type": avro_type
        })

    # Remove `namespace` and `doc` if provided as None or empty
    if namespace:
        schema["namespace"] = namespace
    if doc:
        schema["doc"] = doc

    return parse_schema(schema)

# Function to upload file to Google Cloud Storage
def upload_to_gcs(bucket_name, filename, destination_object):
    try:
        hook = GCSHook(gcp_conn_id=gcp_conn)  # Use the Airflow connection to GCP
        logger.info(f"Attempting to upload {filename} to {bucket_name}/{destination_object}...")
        
        # Upload file to GCS
        hook.upload(bucket_name=bucket_name, filename=filename, object_name=destination_object)
        
        logger.info(f"File successfully uploaded to {bucket_name}/{destination_object}")
    except Exception as e:
        logger.error(f"Failed to upload file to GCS. Error: {e}")
        raise

# Main function to extract data, process, and save as Avro
def extract_and_upload_to_gcs():
    logger.info("Fetching table names from PostgreSQL...")
    table_names = fetch_table_names()  # Fetch table names from PostgreSQL

    # Iterate over each table and process the data
    for table_name in table_names:
        logger.info(f"Processing table {table_name}")
        engine = get_postgresql_connection()  # Get connection
        with engine.connect() as conn:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)

        original_types = df.dtypes
        process_dataframe(df, original_types)  # Process the DataFrame to handle datetimes and other types

        # Data type to Avro mapping
        dtype_to_avro = {
            'object': 'string',         # 'object' -> string
            'int64': 'long',            # 'int64' -> long
            'float64': 'double',        # 'float64' -> double
            'datetime64[ns]': 'string', # 'datetime64[ns]' -> string (or 'long' if you want Unix timestamp)
            'bool': 'boolean',          # 'bool' -> boolean
            'category': 'string',       # 'category' -> string
        }

        # Generate Avro schema
        parsed_schema = get_avro_schema(df, dtype_to_avro, table_name)

        # Save the DataFrame as Avro to a local file
        avro_file_path = f'/tmp/{table_name}.avro'  # Temporary local path
        logger.info(f"Saving Avro file for table {table_name} to {avro_file_path}")
        with open(avro_file_path, 'wb') as out:
            records = df.to_dict(orient='records')
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
            writer(out, parsed_schema, records)
        
        # Upload the Avro file to Google Cloud Storage
        logger.info(f"Uploading Avro file for table {table_name} to GCS...")
        upload_to_gcs('dwh-staging-area', avro_file_path, f'{table_name}.avro')  # Upload to GCS

# Define the DAG
with DAG(
    'postgres_to_gcs_avro',
    default_args={'owner': 'airflow'},
    description='Extract data from PostgreSQL, convert to Avro, and upload to GCS',
    schedule_interval=None,  # You can set a schedule interval if needed
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define the task to run the extraction and upload function
    extract = PythonOperator(
        task_id='extract_and_upload_to_gcs',
        python_callable=extract_and_upload_to_gcs
    )

    extract
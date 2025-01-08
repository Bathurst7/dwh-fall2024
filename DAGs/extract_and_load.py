# airlfow dag
from airflow import DAG

# import datetime module
from datetime import datetime, timedelta, date

# import sql connector
from sqlalchemy import create_engine

# import .avro serialiser
from fastavro import writer, parse_schema

# pandas for data manipulation
import pandas as pd

# import logging module
import logging

# import list of operators
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

# import hook methods
from airflow.hooks.base_hook import BaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# define the global variables for extract and load:
# 1. connection of postgresql
pg_conn = 'cloudsql_postgres_conn'
# 2. connection of google cloud
gcp_conn = 'google_cloud_default'
# 3. bucket name
bucket_name = 'dwh-staging-area'
# 4. project id
project_id = 'dwh2024-fallsem'
# 5. dataset id
dataset_id = 'northwind'

# set-up the logging method
logger = logging.getLogger('airflow.task')

# Extracting to the staging area task
# Step 1: create the engine for the Postgresql connection
def get_postgresql_connection(pg_conn):
    conn = BaseHook.get_connection(pg_conn)
    user_name = conn.login
    password = conn.password
    ip = conn.host
    database_name = conn.schema
    connection_string = f"postgresql+psycopg2://{user_name}:{password}@{ip}:5432/{database_name}"
    return create_engine(connection_string)

# Step 2: fetch the table names from the PostgreSQL database
def fecth_table_names(pg_conn):
    engine = get_postgresql_connection(pg_conn)
    with engine.connect() as conn:
        query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"
        table_names = pd.read_sql(query, conn) # Store the tables in a pandas dataframe
    return table_names['table_name'].tolist() # Convert to list for list comprehension

# Step 3.1: check if a column is datetime-like
# Return binary value (True/False) if the column is datetime-like
def is_datetime_column(df, col):
    non_null_values = df[col].dropna() # Drop null values
    if non_null_values.empty:
        return False
    try:
        pd.to_datetime(non_null_values) # Convert to datetime
        return True
    except (ValueError, TypeError):
        return False

# Step 3.2: process the datetime column
def process_datetime_column(df, original_types):
    for col in df.columns:
        if original_types[col] == 'object': # Check if the column is an object
            if is_datetime_column(df, col): # Check if the column is datetime-like
                df[col] = pd.to_datetime(df[col], errors='coerce')
            else:
                df[col] = df[col].astype('str')
        else:
            pass
    return df

# Step 4: Generate the avro_schema for the table
def get_avro_schema(df, table_name, namespace='staging', doc='northwindoltp'):
    # Data type to Avro mapping
    data_type_to_avro = {
        'object': 'string',         # 'object' -> string
        'int64': 'long',            # 'int64' -> long
        'float64': 'double',        # 'float64' -> double
        'datetime64[ns]': 'string', # 'datetime64[ns]' -> string (or 'long' if you want Unix timestamp)
        'bool': 'boolean',          # 'bool' -> boolean
        'category': 'string',       # 'category' -> string
        'int32': 'int',             # 'int32' -> int
        'float32': 'float',         # 'float32' -> float
        'complex128': 'string',     # Complex numbers as string (no direct Avro type)
        'timedelta[ns]': 'string',  # Timedelta as string (ISO 8601 duration format)
    }
    
    # Create the schema
    schema = {
        'type': 'record',
        'name': table_name,
        'namespace': namespace,
        'doc': doc,
        'fields': []
    }
    
    # add fields based on dataframe coluns and their types
    for col in df.columns:
        dtype = df[col].dtype
        avro_type = data_type_to_avro.get(str(dtype))  # Get mapped type

        # If the dtype is not found, log an error and continue with 'string'
        if avro_type is None:
            logger.info(f"Warning: No direct mapping for dtype {dtype} in column '{col}' in {table_name}, defaulting to 'string'.")
            avro_type = 'string'

        # Check if the column has null values
        if df[col].isnull().any():
            # Allow null in the field (Avro union of null and the field type)
            avro_type = ["null", avro_type]

        # Handle datetime64 columns (pure date vs datetime with time)
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # Pure date column check (without time)
            if df[col].dt.time.isnull().all():
                avro_type = {"type": "int", "logicalType": "date"}  # Pure date columns
            else:
                avro_type = {"type": "long", "logicalType": "timestamp-millis"}  # Datetime with time

            # If the column has nulls, allow null in the field
            if df[col].isnull().any():
                avro_type = ["null", avro_type]

        # Handle timedelta columns explicitly
        elif pd.api.types.is_timedelta64_dtype(df[col]):
            avro_type = {"type": "string", "logicalType": "duration"}  # Timedelta columns

            # If the column has null values, allow null in the field
            if df[col].isnull().any():
                avro_type = ["null", {"type": "string", "logicalType": "duration"}]  # Union type for nullable timedelta columns

        # Handle boolean columns explicitly
        elif pd.api.types.is_bool_dtype(df[col]):
            avro_type = ["null", "boolean"]  # Allow null and map to Avro boolean

        # Append field info to the schema
        schema['fields'].append({
            "name": col,             # Column name
            "type": avro_type        # Mapped Avro type
        })

    # Parse and return the schema using fastavro's parse_schema function
    return parse_schema(schema)

# Step 5: Upload the data to Google Cloud Storage
def upload_to_gcs(bucket_name, filename, destination_object, gcp_conn):
    try:
        hook = GCSHook(gcp_conn)
        logger.info(f"Uploading {filename} to {bucket_name}/{destination_object}")
        hook.upload(bucket_name=bucket_name, filename=filename, object_name=destination_object)
        logger.info(f"Upload successful!")
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise
    
# Step 6: Extract data from PostgreSQL and load to Google Cloud Storage
def extract_and_upload_to_gcs(pg_conn, gcp_conn, bucket_name):
    logger.info("Fetching table names...")
    table_names = fecth_table_names(pg_conn) # this is the list
    logger.info(f"This is the list of tables: {table_names}")
    
    # Iterate over the table names for the extraction phase:
    for table_name in table_names:
        logging.info(f"Extracting data from {table_name}...")
        engine = get_postgresql_connection(pg_conn) # initialise the engine connection
        with engine.connect() as conn:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)
        
        # The table is now stored in the pandas dataframe df
        # Process the datetime columns
        original_types = df.dtypes # store the original types
        process_datetime_column(df, original_types) 
        
        # Generate the avro schema
        parsed_schema = get_avro_schema(df, table_name) # The 2 others are default
        
        # Save the dataframe as Avro to a local file
        avro_file_path=f'/tmp/{table_name}.avro' # Temporary local path
        logger.info(f"Saving the Avro file to {avro_file_path}")
        # Write the dataframe to the avro file
        with open(avro_file_path, 'wb') as outfile:
            records = df.to_dict(orient='records')
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                        # After processing the data, write the data to the avro file
            writer(outfile, parsed_schema, records)
        
        # Upload the Avro file to Google Cloud Storage
        try:
            logger.info(f"Uploading the Avro file {table_name} to GCS...")
            upload_to_gcs(bucket_name, avro_file_path, f'{table_name}.avro', gcp_conn)
            logger.info(f"Upload complete!")
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            raise


# Loading the data to Google BigQuery task
def create_bigquery_tasks(prev_task, bucket_name, project_id, dataset_id,**kwargs):
    # Get the file list from XCom
    file_list = kwargs["ti"].xcom_pull(task_ids=prev_task)
    
    # Log the received file list
    logger.info(f"Files received from GCS: {file_list}")
    if not file_list:
        raise ValueError("No files found in GCS bucket!")
    else:
        logger.info(f"Files found in GCS bucket!")
        for file_name in file_list:
            logger.info(f"Processing file: {file_name}")
            table_name = file_name.split("/")[-1].replace(".avro", "")
            logger.info(f"Target table: {table_name}")
            GCSToBigQueryOperator(
                task_id=f"load_{table_name}_to_bigquery",
                bucket=bucket_name,
                source_objects=[file_name],
                destination_project_dataset_table=f"{project_id}.{dataset_id}.{table_name}",
                source_format="AVRO",
                write_disposition="WRITE_TRUNCATE",
                gcp_conn_id=gcp_conn,
            ).execute(kwargs)

# Define the default arguments
default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id = 'extract',
    default_args=default_args,
    description='Extract data from PostgreSQL and upload to Google Cloud Storage',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['extract', 'load'],
) as dag:
    # task1: extract and upload to gcs
    task1_id = 'task_1_extract_and_upload_to_gcs'
    # task2: list files in gcs
    task2_id = 'task_2_list_files'
    # task3: load to bigquery
    task3_id = 'task_3_load_to_bigquery'
    
    # Phase 1: Extracting data from PostgreSQL and uploading to Google Cloud Storage
    start_extracting = DummyOperator(task_id='start_extracting')
    
    task_extract = PythonOperator(
        task_id=task1_id,
        python_callable=extract_and_upload_to_gcs,
        op_args=[pg_conn, gcp_conn, bucket_name],
        provide_context=True
    )
    end_extracting = DummyOperator(task_id='end_extracting')
    
    # Phase 2: Loading data from Google Cloud Storage to Google BigQuery
    start_loading = DummyOperator(task_id='start_loading')
    
    task_2_list_file = GCSListObjectsOperator(
        task_id=task2_id,
        bucket=bucket_name,
        gcp_conn_id=gcp_conn
    )
    
    task_3_loading = PythonOperator(
        task_id=task3_id,
        python_callable=create_bigquery_tasks,
        op_args=[task2_id, bucket_name, project_id, dataset_id],
        provide_context=True
    )
    
    end_loading = DummyOperator(task_id='end_loading')
    start_extracting >> task_extract >> end_extracting >> start_loading >> task_2_list_file >> task_3_loading >> end_loading
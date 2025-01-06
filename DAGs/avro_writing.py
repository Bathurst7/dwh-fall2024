import pandas as pd
from sqlalchemy import create_engine
from fastavro import writer, parse_schema
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
from google.cloud import storage

pg_conn = 'cloudsql_postgres_conn'
bucket_name = 'dwh-staging-area'

# Function to fetch table names from PostgreSQL using Airflow connection
def fetch_tables_name():
    connection = BaseHook.get_connection(pg_conn)  # Replace 'pg_conn' with your connection ID in Airflow
    connection_string = f"postgresql+psycopg2://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
    engine = create_engine(connection_string)

    with engine.connect() as conn:
        query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"
        table_names = pd.read_sql(query, conn)
    return table_names['table_name'].tolist()

# Function to check if a column is datetime-like
def is_datetime_column(df, col):
    non_null_values = df[col].dropna()
    if non_null_values.empty:
        return False
    try:
        pd.to_datetime(non_null_values, errors='raise')
        return True
    except (ValueError, TypeError):
        return False

# Function to generate Avro schema from a DataFrame
def get_avro_schema(df, data_type_to_avro, table_name, namespace="Northwind-OLTP", doc="Extracted from OLTP"):
    schema = {
        "doc": doc,
        "name": table_name,
        "namespace": namespace,
        "type": "record",
        "fields": []
    }
    for col in df.columns:
        dtype = df[col].dtype
        avro_type = data_type_to_avro.get(str(dtype), 'string')
        if df[col].isnull().any():
            avro_type = ["null", avro_type]
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            avro_type = ["null", "string"]
        elif pd.api.types.is_bool_dtype(df[col]):
            avro_type = ["null", "boolean"]
        schema['fields'].append({"name": col, "type": avro_type})
    return parse_schema(schema)

# Function to process the DataFrame and convert datetime-like columns
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

# Function to extract data and process DataFrame
def extract_and_process_data(bucket_name=bucket_name):
    table_names = fetch_tables_name()

    # Get the connection parameters again for each task run
    connection = BaseHook.get_connection(pg_conn)  # Replace 'pg_conn' with your connection ID in Airflow
    connection_string = f"postgresql+psycopg2://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
    engine = create_engine(connection_string)

    client = storage.Client()
    for table_name in table_names:
        with engine.connect() as conn:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)
        
        original_types = df.dtypes
        process_dataframe(df, original_types)

        parsed_schema = get_avro_schema(df, {}, table_name)

        records = df.to_dict(orient='records')
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None

        # Write to Avro file locally
        local_filename = f"/tmp/{table_name}.avro"
        with open(local_filename, 'wb') as out:
            writer(out, parsed_schema, records)

        # Upload the Avro file to Google Cloud Storage
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"{table_name}.avro")
        blob.upload_from_filename(local_filename)

    return "Writing and uploading to GCS successful."

# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 6),
    'retries': 1,
}

dag = DAG(
    'extract_and_upload_to_gcs',
    default_args=default_args,
    schedule_interval=None,  # Set to a cron expression or None for manual trigger
    catchup=False,
)

# Process and upload data
process_and_upload_task = PythonOperator(
    task_id='extract_process_upload',
    python_callable=extract_and_process_data,
    dag=dag,
)

process_and_upload_task
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
from fastavro import writer, parse_schema
from datetime import datetime

postgres_conn = "cloudsql_postgres_conn"

# Function to fetch table list dynamically
def get_table_list():
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn)

    # Query to fetch table names dynamically
    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
    """
    tables = postgres_hook.get_records(query) 
    return [table[0] for table in tables]  # Extract table names from tuples

# Function to generate AVRO schema for a given table
def generate_avro_schema(table_name):
    postgres_hook = PostgresHook(postgres_conn_id=cloudsql_postgres_conn)
    
    # Query to get column names and data types
    query = f"""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = '{table_name}';
    """
    columns = postgres_hook.get_records(query)

    # Map SQL data types to AVRO types
    sql_to_avro_types = {
        "character varying": "string",
        "text": "string",
        "integer": "int",
        "bigint": "long",
        "numeric": "float",
        "real": "float",
        "double precision": "double",
        "boolean": "boolean",
        "date": "string",
        "timestamp without time zone": "string",
        "timestamp with time zone": "string"
    }

    # Build the schema
    avro_schema = {
        "type": "record",
        "name": f"{table_name}_record",
        "fields": [
            {"name": col[0], "type": sql_to_avro_types.get(col[1], "string")}
            for col in columns
        ],
    }

    return avro_schema

# Function to process a table (extract, transform, and load)
def process_table(table_name, **kwargs):
    # Step 1: Generate AVRO schema for the table
    avro_schema = generate_avro_schema(table_name)

    # Step 2: Extract data
    postgres_hook = PostgresHook(postgres_conn_id="cloudsql_connection")
    query = f"SELECT * FROM {table_name}"
    results = postgres_hook.get_pandas_df(sql=query)

    # Step 3: Write to AVRO format
    avro_file_path = f"/tmp/{table_name}.avro"
    parsed_schema = parse_schema(avro_schema)
    with open(avro_file_path, "wb") as avro_file:
        writer(avro_file, parsed_schema, results.to_dict("records"))
    
    # Step 4: Upload to GCS
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    gcs_bucket_name = "your-gcs-bucket"
    gcs_object_name = f"data/{table_name}.avro"
    gcs_hook.upload(
        bucket_name=gcs_bucket_name,
        object_name=gcs_object_name,
        filename=avro_file_path,
    )
    print(f"File for table {table_name} uploaded to GCS: {gcs_bucket_name}/{gcs_object_name}")

# Define DAG
with DAG(
    dag_id="cloudsql_to_gcs_avro_multiple_tables_dynamic",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    # Step 1: Fetch dynamic table list
    table_list = get_table_list()

    # Step 2: Create tasks for each table dynamically
    for table in table_list:
        PythonOperator(
            task_id=f"process_{table}",
            python_callable=process_table,
            op_kwargs={"table_name": table},
        )
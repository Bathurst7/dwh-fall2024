from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from fastavro import write, parse_schema
import pandas as pd
import tempfile
import os
from datetime import datetime
import logging

# Define global variables
postgres_conn = 'cloudsql_postgres_conn'
gcs_conn = 'google_cloud_default'
gcs_bucket_name = Variable.get('gcs_bucket_name', default_var='dwh-staging-area')


# Fetch table names
def get_table_list(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn)
    query = '''
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
    '''
    tables = postgres_hook.get_records(query)
    table_names = [table[0] for table in tables]
    kwargs['ti'].xcom_push(key='table_list', value=table_names)
    print(f"Fetched tables: {table_names}")  # Add this log for debugging

# Generate Avro schema for a table
def generate_avro_schema(table_name):
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn)
    query = '''
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = %s;
    '''
    columns = postgres_hook.get_records(query, parameters=(table_name,))
    sql_to_avro_types = {
        'character varying': 'string',
        'text': 'string',
        'integer': 'int',
        'numeric': 'float',
        'boolean': 'boolean',
        'date': {'type': 'int', 'logicalType': 'date'},
    }
    avro_schema = {
        'type': 'record',
        'name': f'{table_name}_record',
        'fields': [{'name': col[0], 'type': sql_to_avro_types.get(col[1], 'string')} for col in columns],
    }
    return avro_schema


# Process a single table and upload to GCS
def process_table(table_name, **kwargs):
    avro_schema = generate_avro_schema(table_name)
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn)
    query = f'SELECT * FROM {table_name};'
    results = postgres_hook.get_pandas_df(sql=query)

    with tempfile.TemporaryDirectory() as tmp_dir:
        avro_file_path = os.path.join(tmp_dir, f'{table_name}.avro')
        parsed_schema = parse_schema(avro_schema)
        with open(avro_file_path, 'wb') as avro_file:
            write(avro_file, parsed_schema, results.to_dict('records'))
        gcs_hook = GCSHook(gcp_conn_id=gcs_conn)
        gcs_object_name = f'data/{table_name}.avro'
        gcs_hook.upload(
            bucket_name=gcs_bucket_name,
            object_name=gcs_object_name,
            filename=avro_file_path,
        )
    print(f"File for table '{table_name}' uploaded to GCS: {gcs_bucket_name}/{gcs_object_name}")


# Define the DAG
with DAG(
    dag_id='cloudsql_to_gcs_no_taskgroup',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    # Fetch table list
    fetch_table_list = PythonOperator(
        task_id='fetch_table_list',
        python_callable=get_table_list,
        provide_context=True,
    )

    # Dynamically create tasks for each table
    def create_table_tasks(**kwargs):
        ti = kwargs['ti']
        table_list = ti.xcom_pull(key='table_list', task_ids='fetch_table_list')

        for table_name in table_list:
            PythonOperator(
                task_id=f'process_{table_name}',
                python_callable=process_table,
                op_kwargs={'table_name': table_name},
                dag=dag,  # Ensure tasks are linked to the DAG
            ).set_upstream(fetch_table_list)

    create_table_tasks_op = PythonOperator(
        task_id='create_table_tasks',
        python_callable=create_table_tasks,
        provide_context=True,
    )

    fetch_table_list >> create_table_tasks_op
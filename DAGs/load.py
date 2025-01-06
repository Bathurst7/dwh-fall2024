from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import logging

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Global variables
bucket_name = 'dwh-staging-area'
gcp_conn = 'google_cloud_default'
project_id = 'dwh2024-fallsem'
dataset_id = 'northwind'

with DAG(
    dag_id="loading_gcs_bq",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["dynamic", "gcs", "bigquery"],
) as dag:

    # Task 1: List files in GCS
    list_files = GCSListObjectsOperator(
        task_id="list_files",
        bucket=bucket_name,  # Changed from 'bucket_name' to 'bucket'
        gcp_conn_id=gcp_conn,
    )

    # Dynamically create BigQuery load tasks with logging
    def create_bigquery_tasks(**kwargs):
        # Get the list of files from XCom
        file_list = kwargs["ti"].xcom_pull(task_ids="list_files")
        
        # Log the received file list
        logging.info(f"Files received from GCS: {file_list}")
        
        if not file_list:
            raise ValueError("No files found in GCS bucket!")
        
        # Loop over each file and create the BigQuery load task
        for file_name in file_list:
            # Log each file being processed
            logging.info(f"Processing file: {file_name}")
            
            table_name = file_name.split("/")[-1].replace(".avro", "")
            logging.info(f"Target table: {table_name}")
            
            # Create the BigQuery load task
            GCSToBigQueryOperator(
                task_id=f"load_{table_name}_to_bigquery",
                bucket=bucket_name,
                source_objects=[file_name],
                destination_project_dataset_table=f"{project_id}.{dataset_id}.{table_name}",
                source_format="AVRO",
                write_disposition="WRITE_TRUNCATE",
                gcp_conn_id=gcp_conn,
            ).execute(kwargs)

    # Task 2: Load files into BigQuery using PythonOperator
    load_files_to_bq = PythonOperator(
        task_id="load_files_to_bigquery",
        python_callable=create_bigquery_tasks,
        provide_context=True,
    )

    # Task Dependencies
    list_files >> load_files_to_bq
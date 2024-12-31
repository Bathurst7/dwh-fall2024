from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    'extract_load_postgres',
    default_args={'start_date': datetime(2024, 1, 1)},
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_data = PostgresOperator(
        task_id='extract_data',
        postgres_conn_id='cloudsql_postgres_conn',  # Match your connection ID
        sql='SELECT * FROM INFORMATION_SCHEMA.TABLES;',  # Replace with your query
        dag=dag,
    )

    extract_data
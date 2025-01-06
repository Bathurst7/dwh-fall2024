from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.operators.postgres import SQLExecuteQueryOperator
from datetime import date, datetime, timedelta
from airflow.operators.dummy import DummyOperator

# query from the pg-northwind to get all the tables' name
extract_table_name_query = '''
    SELECT 
        * 
    FROM 
        INFORMATION_SCHEMA.TABLES
    WHERE 
        table_schema = 'public'
        AND table_type = 'BASE TABLE';
'''
# postgres queryable connection:
pg_conn = 'cloudsql_postgres_conn'

with DAG(
    dag_id = 'query_table_name_from_cloud_sql',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:
    task1=DummyOperator(task_id='Starting')
    task2=SQLExecuteQueryOperator(
        task_id ="getting_tables_name",
        sql=extract_table_name_query,
        conn_id = pg_conn
    )

    task1 >> task2
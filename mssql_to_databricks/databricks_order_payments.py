from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import time
import pandas as pd
import tempfile
import os
import json


# Constants
MSSQL_CONN_ID = 'mssql_default'
DATABRICKS_CONN_ID = 'databricks_default'
QUERIES_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config', 'queries.json')
TARGET_TABLE = 'loans_data'
DATABRICKS_CATALOG = 'dqlabs_pov'
DATABRICKS_SCHEMA = 'data_source'
BATCH_SIZE = 1000

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'email_on_failure': True,
    # 'email': ['your-email@example.com'],
    'depends_on_past': False,
}

def read_query_config():
    try:
        with open(QUERIES_CONFIG_PATH, 'r') as file:
            config = json.load(file)
            return config.get('order_items_query', {})
    except Exception as e:
        raise AirflowFailException(f"Error reading query config: {str(e)}")

def validate_mssql_query():
    hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    config = read_query_config()
    query = config.get('query')
    if not query:
        raise AirflowFailException("No query found in configuration")
    try:
        _ = hook.get_pandas_df(f"SELECT TOP 1 * FROM ({query}) AS sub")
    except Exception as e:
        raise AirflowFailException(f"Validation query failed: {str(e)}")

def escape_sql_string(val):
    if val is None or pd.isna(val):
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"

def create_table_if_not_exists():
    config = read_query_config()
    schema = config.get('target_schema', {})
    if not schema:
        raise AirflowFailException("Target schema not found in config")

    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TARGET_TABLE}"
    columns_def = ",\n".join([f"`{col}` {dtype}" for col, dtype in schema.items()])
    create_stmt = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {columns_def}
        )
        USING DELTA
    """

    try:
        hook = DatabricksSqlHook(databricks_conn_id=DATABRICKS_CONN_ID)
        hook.run(create_stmt)
        print(f"Table ensured: {table}")
    except Exception as e:
        raise AirflowFailException(f"Table creation failed: {str(e)}")

def extract_and_insert_data(**context):
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    databricks_hook = DatabricksSqlHook(databricks_conn_id=DATABRICKS_CONN_ID)
    config = read_query_config()

    query = config.get('query')
    schema = config.get('target_schema', {})

    if not query or not schema:
        raise AirflowFailException("Missing query or target schema in configuration")

    df = mssql_hook.get_pandas_df(query)
    total_rows = len(df)
    print(f"Extracted {total_rows} records from MSSQL")

    if df.empty:
        print("No records to insert.")
        return

    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TARGET_TABLE}"
    columns = [f"`{col}`" for col in schema.keys()]
    insert_template = f"INSERT INTO {table} ({', '.join(columns)}) VALUES "

    rows = df.values.tolist()
    for start in range(0, total_rows, BATCH_SIZE):
        batch = rows[start:start + BATCH_SIZE]
        values_clause = ",\n".join([
            "(" + ", ".join([escape_sql_string(val) for val in row]) + ")"
            for row in batch
        ])
        insert_stmt = insert_template + values_clause

        print(f"Inserting rows {start} to {start + len(batch) - 1}...")
        try:
            databricks_hook.run(insert_stmt)
        except Exception as e:
            raise AirflowFailException(f"Failed to insert batch starting at row {start}: {e}")

# Define DAG
with DAG(
    dag_id='databricks_order_payments',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    description='ETL from MSSQL to Databricks using JDBC inserts',
) as dag:

    check_query = PythonOperator(
        task_id='validate_mssql_query',
        python_callable=validate_mssql_query
    )

    ensure_table = PythonOperator(
        task_id='create_table_if_not_exists',
        python_callable=create_table_if_not_exists
    )

    transfer_data = PythonOperator(
        task_id='extract_and_insert_data',
        python_callable=extract_and_insert_data
    )

    check_query >> ensure_table >> transfer_data

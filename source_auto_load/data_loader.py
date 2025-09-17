# dags/sample_data_mssql_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
import csv
from AutoDataLoad.source_auto_load.agent.loader_agent import generate_sample_data

# Configuration - DEFINE HERE
DATABASE_NAME = "pipeline"
SCHEMA_NAME = "source"
TABLE_NAME = "Customers"
MSSQL_CONN_ID = "mssql_default"
GENERATED_FILE = f"/tmp/{TABLE_NAME}_sample_data.csv"

# Airflow DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

# Task: Fetch preview data
def fetch_preview_data(**context):
    hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID, schema=DATABASE_NAME)
    full_table = f"[{SCHEMA_NAME}].[{TABLE_NAME}]"
    sql = f"SELECT TOP 5 * FROM {full_table};"
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    cursor.close()
    conn.close()
    
    context['ti'].xcom_push(key='preview', value={'columns': columns, 'data': rows})

# Task: Generate sample records
def generate_records(**context):
    preview_data = context['ti'].xcom_pull(key='preview')
    records = generate_sample_data(preview_data['data'])
    context['ti'].xcom_push(key='generated_records', value=records)

# Task: Save generated records to CSV
def save_to_file(**context):
    records = context['ti'].xcom_pull(key='generated_records')
    if not records:
        raise ValueError("No generated records found.")
    
    with open(GENERATED_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

# Task: Insert records into MSSQL table
def insert_to_table(**context):
    records = context['ti'].xcom_pull(key='generated_records')
    if not records:
        raise ValueError("No records to insert.")
    
    hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID, schema=DATABASE_NAME)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    full_table = f"[{SCHEMA_NAME}].[{TABLE_NAME}]"
    
    for record in records:
        columns = ', '.join(f"[{key}]" for key in record.keys())
        placeholders = ', '.join(['?'] * len(record))
        values = tuple(record.values())
        sql = f"INSERT INTO {full_table} ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, values)

    conn.commit()
    cursor.close()
    conn.close()

# Define the DAG
with DAG(
    dag_id="daily_sample_data_generator_mssql_static",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_preview_data",
        python_callable=fetch_preview_data,
        provide_context=True
    )

    generate_task = PythonOperator(
        task_id="generate_sample_records",
        python_callable=generate_records,
        provide_context=True
    )

    save_task = PythonOperator(
        task_id="save_generated_file",
        python_callable=save_to_file,
        provide_context=True
    )

    insert_task = PythonOperator(
        task_id="insert_generated_data",
        python_callable=insert_to_table,
        provide_context=True
    )

    # Task flow
    fetch_task >> generate_task >> save_task >> insert_task

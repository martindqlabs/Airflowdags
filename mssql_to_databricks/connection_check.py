from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException

from datetime import datetime

def check_databricks_schema_access():
    hook = DatabricksSqlHook(databricks_conn_id='databricks_default')
    
    try:
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Simple connectivity check
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result[0] != 1:
                    raise AirflowFailException("Basic connection test failed.")
                print("✅ Basic connection to Databricks successful.")

                # Schema access check
                for schema in ['banking_data_source']:
                    try:
                        cursor.execute(f"SHOW TABLES IN {schema}")
                        tables = cursor.fetchall()
                        print(f"✅ Access to schema `{schema}` successful. Tables: {[row[1] for row in tables]}")
                    except Exception as schema_err:
                        raise AirflowFailException(f"❌ Cannot access schema `{schema}`: {str(schema_err)}")

    except Exception as e:
        raise AirflowFailException(f"❌ Databricks schema access test failed: {str(e)}")

with DAG(
    dag_id='databricks_schema_access_check',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['databricks', 'schema_check'],
) as dag:

    test_schema_access = PythonOperator(
        task_id='check_schema_access',
        python_callable=check_databricks_schema_access,
    )

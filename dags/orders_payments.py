from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowFailException
from snowflake.connector.errors import DatabaseError, OperationalError
from datetime import datetime, timedelta
import time
import pandas as pd
import tempfile
import os
import json

# Constants
MSSQL_CONN_ID = 'mssql_default'
SNOWFLAKE_CONN_ID = 'snowflake_default'
QUERIES_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config', 'queries.json')
TARGET_TABLE = 'order_payments'
SNOWFLAKE_STAGE = 'PIPELINE_STAGE'
SNOWFLAKE_DATABASE = 'DQLABS_QA'  # Replace with your database
SNOWFLAKE_SCHEMA = 'STAGING'     # Replace with your schema
MAX_RETRIES = 1
RETRY_DELAY = 10  # seconds

def get_snowflake_connection():
    """Get Snowflake connection with retry logic."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    for attempt in range(MAX_RETRIES):
        try:
            conn = hook.get_conn()
            return conn
        except (OperationalError, ConnectionRefusedError) as e:
            if attempt == MAX_RETRIES - 1:
                raise AirflowFailException(f"Failed to connect to Snowflake after {MAX_RETRIES} attempts: {str(e)}")
            print(f"Connection attempt {attempt + 1} failed. Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)

def read_query_config():
    """Read the SQL query configuration from JSON file."""
    try:
        with open(QUERIES_CONFIG_PATH, 'r') as file:
            config = json.load(file)
            return config.get('order_items_query', {})
    except FileNotFoundError:
        raise AirflowFailException(f"Query configuration file not found at {QUERIES_CONFIG_PATH}")
    except json.JSONDecodeError:
        raise AirflowFailException(f"Invalid JSON format in {QUERIES_CONFIG_PATH}")
    except Exception as e:
        raise AirflowFailException(f"Error reading query configuration: {e}")

def generate_create_table_sql(schema):
    """Generate CREATE TABLE SQL statement from schema."""
    columns = []
    for column_name, data_type in schema.items():
        columns.append(f"{column_name} {data_type}")
    return f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        {', '.join(columns)}
    );
    """

# DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['your-email@example.com'],  # Add your email for notifications
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
}

with DAG(
    dag_id='snowflake_order_payments',
    default_args=default_args,
    schedule_interval='0 */12 * * *',  # Run every 12 hours
    start_date=days_ago(1),
    catchup=False,
    description='ETL from MSSQL to Snowflake',
) as dag:

    def validate_mssql_query():
        hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        try:
            config = read_query_config()
            query = config.get('query')
            print(query)
            if not query:
                raise AirflowFailException("No query found in configuration")
            _ = hook.get_pandas_df(f"SELECT TOP 1 * FROM ({query}) AS sub")
        except Exception as e:
            raise AirflowFailException(f"Query failed: {e}")

    def extract_data_from_mssql(**context):
        hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        config = read_query_config()
        query = config.get('query')
        if not query:
            raise AirflowFailException("No query found in configuration")
        
        # Get the data
        df = hook.get_pandas_df(query)
        
        # Print preview and record count
        print("\n=== Data Preview ===")
        print(f"Total number of records: {len(df)}")
        print("\nFirst 5 rows:")
        print(df.head().to_string())
        print("\nDataFrame Info:")
        print(df.info())
        
        # Save to CSV
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        df.to_csv(tmp_file.name, index=False)
        context['ti'].xcom_push(key='csv_path', value=tmp_file.name)
        
        # Log the file path for debugging
        print(f"\nCSV file saved to: {tmp_file.name}")

    def upload_to_snowflake_stage(**context):
        csv_path = context['ti'].xcom_pull(key='csv_path')
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        # Count records in CSV file
        try:
            with open(csv_path, 'r') as file:
                # Subtract 1 for header row
                record_count = sum(1 for _ in file) - 1
                print(f"\n=== CSV File Statistics ===")
                print(f"Total number of records in CSV: {record_count}")
                print(f"CSV file path: {csv_path}")
        except Exception as e:
            print(f"Error reading CSV file: {str(e)}")
        
        # Get the Snowflake connection
        conn = hook.get_conn()
        cur = conn.cursor()
        
        try:
            # Create the stage if it doesn't exist
            create_stage_sql = f"""
            CREATE STAGE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
            """
            cur.execute(create_stage_sql)
            
            # Upload the file to the stage
            put_sql = f"PUT file://{csv_path} @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}"
            cur.execute(put_sql)
            
            # Commit the transaction
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise AirflowFailException(f"Failed to upload file to Snowflake stage: {str(e)}")
        finally:
            cur.close()
            conn.close()

    def ensure_target_table_exists():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        config = read_query_config()
        schema = config.get('target_schema', {})
        if not schema:
            raise AirflowFailException("No schema found in configuration")
        
        # Get the Snowflake connection
        conn = hook.get_conn()
        cur = conn.cursor()
        
        try:
            create_stmt = f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TARGET_TABLE} (
                {', '.join(f"{column_name} {data_type}" for column_name, data_type in schema.items())}
            );
            """
            cur.execute(create_stmt)
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise AirflowFailException(f"Failed to create target table: {str(e)}")
        finally:
            cur.close()
            conn.close()

    def copy_into_target_table(**context):
        csv_path = context['ti'].xcom_pull(key='csv_path')
        file_name = os.path.basename(csv_path)
        
        try:
            # Get connection with retry logic
            conn = get_snowflake_connection()
            cur = conn.cursor()
            
            try:
                # First, get the count before load
                print("\n=== Starting COPY Operation ===")
                cur.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TARGET_TABLE}")
                result = cur.fetchone()
                count_before = result[0] if result else 0
                print(f"Records in target table before load: {count_before}")
                
                # Perform the COPY operation
                copy_stmt = f"""
                        COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TARGET_TABLE}
                        FROM @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}
                        FILES = ('{file_name}.gz')
                        FILE_FORMAT = (
                            TYPE = 'CSV',
                            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
                            SKIP_HEADER = 1,
                            FIELD_DELIMITER = ',',
                            COMPRESSION = 'GZIP'
                        )
                        ON_ERROR = 'ABORT_STATEMENT';
                        """

                print(f"\nExecuting COPY command: {copy_stmt}")
                cur.execute(copy_stmt)
                
                # Get the count after load
                print("\nFetching post-load count...")
                cur.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TARGET_TABLE}")
                result = cur.fetchone()
                count_after = result[0] if result else 0
                
                conn.commit()
                print("\n=== COPY Operation Completed ===")
                
            except Exception as e:
                conn.rollback()
                print(f"Error during COPY operation: {str(e)}")
                raise AirflowFailException(f"Failed to copy data into target table: {str(e)}")
            finally:
                cur.close()
                conn.close()
                
        except Exception as e:
            print(f"Error in copy_into_target_table: {str(e)}")
            raise AirflowFailException(f"Failed to process data: {str(e)}")

    check_query = PythonOperator(
        task_id='validate_mssql_query',
        python_callable=validate_mssql_query
    )

    extract_data = PythonOperator(
        task_id='extract_data_from_mssql',
        python_callable=extract_data_from_mssql
    )

    upload_stage = PythonOperator(
        task_id='upload_to_snowflake_stage',
        python_callable=upload_to_snowflake_stage
    )

    ensure_target = PythonOperator(
        task_id='ensure_target_table_exists',
        python_callable=ensure_target_table_exists
    )

    copy_data = PythonOperator(
        task_id='copy_into_target_table',
        python_callable=copy_into_target_table
    )

    check_query >> extract_data >> upload_stage >> ensure_target >> copy_data
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
from faker import Faker
import random
import uuid

MSSQL_CONN_ID = 'mssql_default'
DATABASE = 'pipeline'
SCHEMA = 'source'

fake = Faker()

def get_mssql_hook():
    return MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)

def generate_customer_data():
    num_customers = random.randint(10, 1000)
    return [
        (
            str(uuid.uuid4()), fake.first_name(), fake.last_name(),
            fake.email(), f"{fake.phone_number()}x{fake.random_int(100,999)}",
            fake.date_between('-2y', 'today').strftime('%Y-%m-%d'),
            random.choice(['Active', 'Inactive', 'Pending']),
            random.randint(0, 5000)
        )
        for _ in range(num_customers)
    ]

def generate_addresses(customer_ids):
    num_records = random.randint(10, 1000)
    return [
        (
            str(uuid.uuid4()), random.choice(customer_ids),
            fake.street_address(), fake.city(), fake.state_abbr(),
            fake.postcode(), fake.country()
        )
        for _ in range(num_records)
    ]

def generate_orders(customer_ids):
    num_records = random.randint(10, 1000)
    return [
        (
            str(uuid.uuid4()), random.choice(customer_ids),
            fake.date_between('-1y', 'today').strftime('%Y-%m-%d'),
            random.choice(['Placed', 'Shipped', 'Delivered', 'Cancelled']),
            round(random.uniform(10, 1000), 2)
        )
        for _ in range(num_records)
    ]

def generate_payments(order_ids):
    num_records = random.randint(10, 1000)
    return [
        (
            str(uuid.uuid4()), random.choice(order_ids),
            fake.date_between('-6m', 'today').strftime('%Y-%m-%d'),
            round(random.uniform(10, 1000), 2),
            random.choice(['Credit Card', 'PayPal', 'Bank Transfer'])
        )
        for _ in range(num_records)
    ]

def generate_support_tickets(customer_ids):
    num_records = random.randint(10, 1000)
    return [
        (
            str(uuid.uuid4()), random.choice(customer_ids),
            random.choice(['Technical', 'Billing', 'Other']),
            fake.sentence(), fake.date_between('-6m', 'today').strftime('%Y-%m-%d'),
            random.choice(['Open', 'Closed', 'Pending'])
        )
        for _ in range(num_records)
    ]

def generate_order_items(order_ids):
    num_records = random.randint(10, 1000)
    return [
        (
            str(uuid.uuid4()), random.choice(order_ids),
            fake.word(), random.randint(1, 10),
            round(random.uniform(10, 200), 2)
        )
        for _ in range(num_records)
    ]

def insert_data_to_table(table, columns, data):
    hook = get_mssql_hook()
    full_table_name = f"{DATABASE}.{SCHEMA}.{table}"
    hook.insert_rows(table=full_table_name, rows=data, target_fields=columns, replace=False)
    print(f"Inserted {len(data)} records into {table}.")

def insert_customers(**kwargs):
    data = generate_customer_data()
    kwargs['ti'].xcom_push(key='customer_ids', value=[row[0] for row in data])
    kwargs['ti'].xcom_push(key='customer_count', value=len(data))
    insert_data_to_table('Customers', [
        'CustomerID', 'FirstName', 'LastName', 'Email', 'PhoneNumber', 'JoinDate', 'Status', 'LoyaltyPoints'
    ], data)

def insert_addresses(**kwargs):
    customer_ids = kwargs['ti'].xcom_pull(key='customer_ids', task_ids='insert_customers')
    data = generate_addresses(customer_ids)
    kwargs['ti'].xcom_push(key='address_count', value=len(data))
    insert_data_to_table('Customer_Addresses', [
        'AddressID', 'CustomerID', 'Street', 'City', 'State', 'PostalCode', 'Country'
    ], data)

def insert_orders(**kwargs):
    customer_ids = kwargs['ti'].xcom_pull(key='customer_ids', task_ids='insert_customers')
    data = generate_orders(customer_ids)
    kwargs['ti'].xcom_push(key='order_ids', value=[row[0] for row in data])
    kwargs['ti'].xcom_push(key='order_count', value=len(data))
    insert_data_to_table('Customer_Orders', [
        'OrderID', 'CustomerID', 'OrderDate', 'OrderStatus', 'TotalAmount'
    ], data)

def insert_payments(**kwargs):
    order_ids = kwargs['ti'].xcom_pull(key='order_ids', task_ids='insert_orders')
    data = generate_payments(order_ids)
    kwargs['ti'].xcom_push(key='payment_count', value=len(data))
    insert_data_to_table('Customer_Payments', [
        'PaymentID', 'OrderID', 'PaymentDate', 'PaymentAmount', 'PaymentMethod'
    ], data)

def insert_tickets(**kwargs):
    customer_ids = kwargs['ti'].xcom_pull(key='customer_ids', task_ids='insert_customers')
    data = generate_support_tickets(customer_ids)
    kwargs['ti'].xcom_push(key='ticket_count', value=len(data))
    insert_data_to_table('Customer_Support_Tickets', [
        'TicketID', 'CustomerID', 'IssueType', 'Description', 'TicketDate', 'ResolutionStatus'
    ], data)

def insert_order_items(**kwargs):
    order_ids = kwargs['ti'].xcom_pull(key='order_ids', task_ids='insert_orders')
    data = generate_order_items(order_ids)
    kwargs['ti'].xcom_push(key='item_count', value=len(data))
    insert_data_to_table('Order_Items', [
        'OrderItemID', 'OrderID', 'ProductName', 'Quantity', 'PricePerItem'
    ], data)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    'populate_mssql_customer_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id='insert_customers', python_callable=insert_customers, provide_context=True)
    t2 = PythonOperator(task_id='insert_addresses', python_callable=insert_addresses, provide_context=True)
    t3 = PythonOperator(task_id='insert_orders', python_callable=insert_orders, provide_context=True)
    t4 = PythonOperator(task_id='insert_payments', python_callable=insert_payments, provide_context=True)
    t5 = PythonOperator(task_id='insert_tickets', python_callable=insert_tickets, provide_context=True)
    t6 = PythonOperator(task_id='insert_order_items', python_callable=insert_order_items, provide_context=True)

    t1 >> [t2, t3, t5]
    t3 >> [t4, t6]

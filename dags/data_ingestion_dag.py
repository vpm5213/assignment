"""
DAG for ingesting Brazilian E-Commerce Dataset from CSV files into PostgreSQL
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import logging

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ecommerce_data_ingestion',
    default_args=default_args,
    description='Load Brazilian E-Commerce data from CSV to PostgreSQL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['ecommerce', 'etl'],
)

# Map of CSV files to table names
file_to_table_mapping = {
    'olist_customers_dataset.csv': 'customers',
    'olist_geolocation_dataset.csv': 'geolocation',
    'olist_order_items_dataset.csv': 'order_items',
    'olist_order_payments_dataset.csv': 'order_payments',
    'olist_order_reviews_dataset.csv': 'order_reviews',
    'olist_orders_dataset.csv': 'orders',
    'olist_products_dataset.csv': 'products',
    'olist_sellers_dataset.csv': 'sellers',
    'product_category_name_translation.csv': 'category_translation'
}

# Directory where CSV files are stored
DATA_PATH = '/opt/airflow/data/raw/'

def create_tables():
    """Create tables in PostgreSQL if they don't exist"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # SQL commands to create tables based on the CSV structure
    create_tables_sql = """
    -- Customers table
    CREATE TABLE IF NOT EXISTS customers (
        customer_id VARCHAR(255) PRIMARY KEY,
        customer_unique_id VARCHAR(255),
        customer_zip_code_prefix VARCHAR(50),
        customer_city VARCHAR(255),
        customer_state VARCHAR(2)
    );
    
    -- Geolocation table
    CREATE TABLE IF NOT EXISTS geolocation (
        geolocation_zip_code_prefix VARCHAR(50),
        geolocation_lat NUMERIC,
        geolocation_lng NUMERIC,
        geolocation_city VARCHAR(255),
        geolocation_state VARCHAR(2)
    );
    
    -- Orders table
    CREATE TABLE IF NOT EXISTS orders (
        order_id VARCHAR(255) PRIMARY KEY,
        customer_id VARCHAR(255),
        order_status VARCHAR(50),
        order_purchase_timestamp TIMESTAMP,
        order_approved_at TIMESTAMP,
        order_delivered_carrier_date TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP
    );
    
    -- Order Items table
    CREATE TABLE IF NOT EXISTS order_items (
        order_id VARCHAR(255),
        order_item_id INT,
        product_id VARCHAR(255),
        seller_id VARCHAR(255),
        shipping_limit_date TIMESTAMP,
        price NUMERIC,
        freight_value NUMERIC,
        PRIMARY KEY (order_id, order_item_id)
    );
    
    -- Products table
    CREATE TABLE IF NOT EXISTS products (
        product_id VARCHAR(255) PRIMARY KEY,
        product_category_name VARCHAR(255),
        product_name_length INT,
        product_description_length INT,
        product_photos_qty INT,
        product_weight_g INT,
        product_length_cm INT,
        product_height_cm INT,
        product_width_cm INT
    );
    
    -- Sellers table
    CREATE TABLE IF NOT EXISTS sellers (
        seller_id VARCHAR(255) PRIMARY KEY,
        seller_zip_code_prefix VARCHAR(50),
        seller_city VARCHAR(255),
        seller_state VARCHAR(2)
    );
    
    -- Order payments table
    CREATE TABLE IF NOT EXISTS order_payments (
        order_id VARCHAR(255),
        payment_sequential INT,
        payment_type VARCHAR(50),
        payment_installments INT,
        payment_value NUMERIC,
        PRIMARY KEY (order_id, payment_sequential)
    );
    
    -- Order reviews table
    CREATE TABLE IF NOT EXISTS order_reviews (
        review_id VARCHAR(255),
        order_id VARCHAR(255),
        review_score INT,
        review_comment_title VARCHAR(255),
        review_comment_message TEXT,
        review_creation_date TIMESTAMP,
        review_answer_timestamp TIMESTAMP
    );
    
    -- Category translation table
    CREATE TABLE IF NOT EXISTS category_translation (
        product_category_name VARCHAR(255),
        product_category_name_english VARCHAR(255)
    );
    
    -- KPI results table
    CREATE TABLE IF NOT EXISTS kpi_results (
        kpi_name VARCHAR(100),
        kpi_value NUMERIC,
        dimension VARCHAR(100),
        dimension_value VARCHAR(255),
        calculation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(create_tables_sql)
    conn.commit()
    cursor.close()
    conn.close()
    
    logging.info("Tables created successfully")

def load_csv_to_postgres(file_name, table_name):
    """
    Load data from a CSV file into PostgreSQL table
    """
    try:
        file_path = os.path.join(DATA_PATH, file_name)
        logging.info(f"Processing file: {file_path}")
        
        # Read CSV file
        df = pd.read_csv(file_path, low_memory=False)
        
        # Clean column names (remove spaces, special chars)
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        # Handle date columns for specific tables
        date_columns = []
        if table_name == 'orders':
            date_columns = ['order_purchase_timestamp', 'order_approved_at', 
                           'order_delivered_carrier_date', 'order_delivered_customer_date', 
                           'order_estimated_delivery_date']
        elif table_name == 'order_items':
            date_columns = ['shipping_limit_date']
        elif table_name == 'order_reviews':
            date_columns = ['review_creation_date', 'review_answer_timestamp']
            
        # Convert date columns to datetime
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Connect to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        
        # Use pandas to_sql to insert data
        df.to_sql(
            table_name, 
            conn, 
            if_exists='replace', 
            index=False,
            method='multi',
            chunksize=1000
        )
        
        conn.close()
        logging.info(f"Successfully loaded {len(df)} rows into {table_name}")
        return f"Loaded {len(df)} rows into {table_name}"
    
    except Exception as e:
        logging.error(f"Error loading {file_name} to {table_name}: {str(e)}")
        raise

# Task to create tables
create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag,
)

# Create tasks for each CSV file
load_tasks = []
for file_name, table_name in file_to_table_mapping.items():
    task = PythonOperator(
        task_id=f'load_{table_name}_data',
        python_callable=load_csv_to_postgres,
        op_kwargs={'file_name': file_name, 'table_name': table_name},
        dag=dag,
    )
    load_tasks.append(task)

# Define task dependencies
create_tables_task >> load_tasks
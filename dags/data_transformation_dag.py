"""
DAG for transforming e-commerce data and calculating business KPIs
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import logging

# Create directory for visualizations
os.makedirs('/opt/airflow/visualizations', exist_ok=True)

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
    'ecommerce_data_transformation',
    default_args=default_args,
    description='Transform e-commerce data and calculate KPIs',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['ecommerce', 'kpi', 'analysis'],
)

def calculate_customer_lifetime_value():
    """
    Calculate Customer Lifetime Value (CLV) KPI
    CLV = Average Order Value × Purchase Frequency × Customer Lifespan
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # SQL to calculate CLV components
        sql = """
        WITH customer_orders AS (
            SELECT 
                c.customer_unique_id,
                COUNT(DISTINCT o.order_id) AS num_orders,
                SUM(p.payment_value) AS total_spent,
                MIN(o.order_purchase_timestamp) AS first_purchase,
                MAX(o.order_purchase_timestamp) AS last_purchase
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            JOIN order_payments p ON o.order_id = p.order_id
            WHERE o.order_status = 'delivered'
            GROUP BY c.customer_unique_id
        ),
        customer_metrics AS (
            SELECT 
                customer_unique_id,
                num_orders,
                total_spent,
                total_spent / num_orders AS avg_order_value,
                EXTRACT(EPOCH FROM (last_purchase - first_purchase)) / 86400 AS customer_age_days
            FROM customer_orders
            WHERE num_orders > 1
        )
        SELECT 
            cm.customer_unique_id,
            cm.num_orders,
            cm.avg_order_value,
            CASE 
                WHEN cm.customer_age_days > 0 THEN cm.num_orders / cm.customer_age_days * 30 
                ELSE 1 
            END AS monthly_frequency,
            cm.customer_age_days,
            cm.avg_order_value * (CASE WHEN cm.customer_age_days > 0 THEN cm.num_orders / cm.customer_age_days * 30 ELSE 1 END) * 12 AS annual_clv
        FROM customer_metrics cm
        WHERE cm.customer_age_days > 0
        """
        
        # Execute the query
        df = pg_hook.get_pandas_df(sql)
        
        # Calculate overall averages for KPI table
        avg_clv = df['annual_clv'].mean()
        
        # Store results in KPI table
        insert_sql = """
        INSERT INTO kpi_results (kpi_name, kpi_value, dimension, dimension_value)
        VALUES (%s, %s, %s, %s)
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(insert_sql, ('Customer Lifetime Value', avg_clv, 'overall', 'all'))
        
        # Also calculate CLV by state
        state_sql = """
        WITH customer_orders AS (
            SELECT 
                c.customer_unique_id,
                c.customer_state,
                COUNT(DISTINCT o.order_id) AS num_orders,
                SUM(p.payment_value) AS total_spent,
                MIN(o.order_purchase_timestamp) AS first_purchase,
                MAX(o.order_purchase_timestamp) AS last_purchase
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            JOIN order_payments p ON o.order_id = p.order_id
            WHERE o.order_status = 'delivered'
            GROUP BY c.customer_unique_id, c.customer_state
        ),
        state_metrics AS (
            SELECT 
                customer_state,
                AVG(total_spent / num_orders) AS avg_order_value,
                AVG(CASE 
                    WHEN EXTRACT(EPOCH FROM (last_purchase - first_purchase)) / 86400 > 0 
                    THEN num_orders / (EXTRACT(EPOCH FROM (last_purchase - first_purchase)) / 86400) * 30
                    ELSE 1 
                END) AS avg_monthly_frequency
            FROM customer_orders
            WHERE num_orders > 1
            GROUP BY customer_state
        )
        SELECT 
            customer_state,
            avg_order_value,
            avg_monthly_frequency,
            avg_order_value * avg_monthly_frequency * 12 AS avg_annual_clv
        FROM state_metrics
        """
        
        state_df = pg_hook.get_pandas_df(state_sql)
        
        # Insert state-level CLV into KPI table
        for _, row in state_df.iterrows():
            cursor.execute(insert_sql, (
                'Customer Lifetime Value', 
                row['avg_annual_clv'], 
                'state', 
                row['customer_state']
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Create visualization
        plt.figure(figsize=(12, 6))
        top_states = state_df.sort_values('avg_annual_clv', ascending=False).head(10)
        sns.barplot(x='customer_state', y='avg_annual_clv', data=top_states)
        plt.title('Average Annual Customer Lifetime Value by State (Top 10)')
        plt.xlabel('State')
        plt.ylabel('Annual CLV (BRL)')
        plt.tight_layout()
        plt.savefig('/opt/airflow/visualizations/clv_by_state.png')
        plt.close()
        
        logging.info("CLV calculation completed successfully")
        
    except Exception as e:
        logging.error(f"Error calculating CLV: {str(e)}")
        raise

def calculate_product_return_rate():
    """
    Calculate Product Return Rate KPI
    Return Rate = Orders with status 'canceled' or specific return indicators / Total Orders
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # SQL to calculate return rate
        sql = """
        WITH product_orders AS (
            SELECT 
                p.product_id,
                p.product_category_name,
                COUNT(DISTINCT o.order_id) AS total_orders,
                COUNT(DISTINCT CASE WHEN o.order_status = 'canceled' THEN o.order_id END) AS canceled_orders
            FROM products p
            JOIN order_items oi ON p.product_id = oi.product_id
            JOIN orders o ON oi.order_id = o.order_id
            GROUP BY p.product_id, p.product_category_name
        )
        SELECT 
            product_id,
            product_category_name,
            total_orders,
            canceled_orders,
            CASE 
                WHEN total_orders > 0 THEN ROUND((canceled_orders::NUMERIC / total_orders) * 100, 2)
                ELSE 0 
            END AS return_rate
        FROM product_orders
        WHERE total_orders >= 5  -- Only consider products with sufficient orders
        """
        
        # Execute the query
        df = pg_hook.get_pandas_df(sql)
        
        # Join with category translation
        category_sql = "SELECT * FROM category_translation"
        category_df = pg_hook.get_pandas_df(category_sql)
        
        # Merge to get English category names
        if not category_df.empty and 'product_category_name' in category_df.columns:
            df = pd.merge(
                df, 
                category_df, 
                on='product_category_name', 
                how='left'
            )
        
        # Calculate overall average return rate
        overall_return_rate = df['return_rate'].mean()
        
        # Calculate category-level return rates
        category_return_rates = df.groupby('product_category_name').agg({
            'total_orders': 'sum',
            'canceled_orders': 'sum'
        })
        
        category_return_rates['return_rate'] = (category_return_rates['canceled_orders'] / 
                                               category_return_rates['total_orders'] * 100).round(2)
        
        # Store results in KPI table
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Overall return rate
        cursor.execute(
            "INSERT INTO kpi_results (kpi_name, kpi_value, dimension, dimension_value) VALUES (%s, %s, %s, %s)",
            ('Product Return Rate', overall_return_rate, 'overall', 'all')
        )
        
        # Category return rates
        for category, row in category_return_rates.iterrows():
            category_name = category if pd.notna(category) else 'unknown'
            cursor.execute(
                "INSERT INTO kpi_results (kpi_name, kpi_value, dimension, dimension_value) VALUES (%s, %s, %s, %s)",
                ('Product Return Rate', row['return_rate'], 'category', category_name)
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Create visualization - Top 10 categories with highest return rates
        plt.figure(figsize=(12, 6))
        top_categories = category_return_rates.sort_values('return_rate', ascending=False).head(10)
        sns.barplot(x=top_categories.index, y=top_categories['return_rate'])
        plt.title('Top 10 Product Categories by Return Rate')
        plt.xlabel('Product Category')
        plt.ylabel('Return Rate (%)')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig('/opt/airflow/visualizations/return_rate_by_category.png')
        plt.close()
        
        logging.info("Return rate calculation completed successfully")
        
    except Exception as e:
        logging.error(f"Error calculating return rate: {str(e)}")
        raise

def calculate_sales_by_region():
    """
    Calculate Sales by Region KPI
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # SQL to calculate sales by region/state
        sql = """
        SELECT 
            c.customer_state AS state,
            COUNT(DISTINCT o.order_id) AS order_count,
            SUM(p.payment_value) AS total_sales,
            AVG(p.payment_value) AS avg_order_value,
            COUNT(DISTINCT c.customer_id) AS customer_count
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN order_payments p ON o.order_id = p.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY c.customer_state
        ORDER BY total_sales DESC
        """
        
        # Execute the query
        df = pg_hook.get_pandas_df(sql)
        
        # Calculate additional metrics
        df['sales_per_customer'] = df['total_sales'] / df['customer_count']
        
        # Store results in KPI table
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO kpi_results (kpi_name, kpi_value, dimension, dimension_value) VALUES (%s, %s, %s, %s)",
                ('Sales by Region', row['total_sales'], 'state', row['state'])
            )
            
            cursor.execute(
                "INSERT INTO kpi_results (kpi_name, kpi_value, dimension, dimension_value) VALUES (%s, %s, %s, %s)",
                ('Orders by Region', row['order_count'], 'state', row['state'])
            )
            
            cursor.execute(
                "INSERT INTO kpi_results (kpi_name, kpi_value, dimension, dimension_value) VALUES (%s, %s, %s, %s)",
                ('Sales per Customer', row['sales_per_customer'], 'state', row['state'])
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Create visualization
        plt.figure(figsize=(12, 6))
        top_states = df.sort_values('total_sales', ascending=False).head(10)
        sns.barplot(x='state', y='total_sales', data=top_states)
        plt.title('Total Sales by State (Top 10)')
        plt.xlabel('State')
        plt.ylabel('Total Sales (BRL)')
        plt.tight_layout()
        plt.savefig('/opt/airflow/visualizations/sales_by_state.png')
        plt.close()
        
        # Additional visualization - sales per customer
        plt.figure(figsize=(12, 6))
        top_states_per_customer = df.sort_values('sales_per_customer', ascending=False).head(10)
        sns.barplot(x='state', y='sales_per_customer', data=top_states_per_customer)
        plt.title('Sales per Customer by State (Top 10)')
        plt.xlabel('State')
        plt.ylabel('Sales per Customer (BRL)')
        plt.tight_layout()
        plt.savefig('/opt/airflow/visualizations/sales_per_customer_by_state.png')
        plt.close()
        
        logging.info("Sales by region calculation completed successfully")
        
    except Exception as e:
        logging.error(f"Error calculating sales by region: {str(e)}")
        raise

# Define tasks for KPI calculations
clv_task = PythonOperator(
    task_id='calculate_customer_lifetime_value',
    python_callable=calculate_customer_lifetime_value,
    dag=dag,
)

return_rate_task = PythonOperator(
    task_id='calculate_product_return_rate',
    python_callable=calculate_product_return_rate,
    dag=dag,
)

sales_by_region_task = PythonOperator(
    task_id='calculate_sales_by_region',
    python_callable=calculate_sales_by_region,
    dag=dag,
)

# Define task dependencies - these can run in parallel
[clv_task, return_rate_task, sales_by_region_task]
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
import psycopg2
from urllib.parse import quote_plus

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for processing ecommerce data',
    schedule_interval='@once',
    start_date=datetime(2024, 12, 26),
    catchup=False,
)

def extract_data(**kwargs):
    """Extract data from incoming files."""
    input_dir = '/Users/panda/Documents/ETL_pipeline_and_dashboard/incoming'
    files = os.listdir(input_dir)
    if not files:
        raise Exception("No files to process")
    file_to_process = os.path.join(input_dir, files[0])
    return file_to_process

def transform_data(file_path, **kwargs):
    """Clean and transform the data."""
    data = pd.read_csv(file_path)
    data = data.dropna()
    cleaned_path = file_path.replace('incoming', 'processed')
    os.makedirs('processed', exist_ok=True)
    data.to_csv(cleaned_path, index=False)
    return cleaned_path

def initialize_database():
    """Create database tables if they don't exist"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        order_id BIGINT,
        user_id INTEGER,
        order_number SMALLINT,
        order_dow SMALLINT,
        order_hour_of_day SMALLINT,
        days_since_prior_order FLOAT,
        product_id INTEGER,
        add_to_cart_order SMALLINT,
        reordered BOOLEAN,
        department_id SMALLINT,
        department VARCHAR(20),
        product_name VARCHAR(100)
    );
    """
    
    db_params = {
        'user': 'postgres',
        'password': quote_plus('england@chelsea'),
        'host': 'localhost',
        'port': '5434',
        'database': 'ecommerce'
    }
    
    conn = psycopg2.connect(
        dbname=db_params['database'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host'],
        port=db_params['port']
    )
    
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def load_data(cleaned_file, **kwargs):
    """Load the transformed data into Postgres."""
    # Read and prepare data
    data = pd.read_csv(cleaned_file)
    
    # Convert numpy data types to Python native types
    data = data.astype({
        'order_id': 'int',
        'user_id': 'int',
        'order_number': 'int',
        'order_dow': 'int',
        'order_hour_of_day': 'int',
        'days_since_prior_order': 'float',
        'product_id': 'int',
        'add_to_cart_order': 'int',
        'reordered': 'bool',
        'department_id': 'int',
        'department': 'str',
        'product_name': 'str'
    })
    
    # Database connection parameters
    db_params = {
        'user': 'postgres',
        'password': quote_plus('england@chelsea'),
        'host': 'localhost',
        'port': '5434',
        'database': 'ecommerce'
    }
    
    # Create connection
    conn = psycopg2.connect(
        dbname=db_params['database'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host'],
        port=db_params['port']
    )
    
    try:
        # Create a cursor
        with conn.cursor() as cur:
            # Prepare the insert query
            insert_query = """
            INSERT INTO orders (
                order_id, user_id, order_number, order_dow, 
                order_hour_of_day, days_since_prior_order, product_id,
                add_to_cart_order, reordered, department_id, 
                department, product_name
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            # Convert DataFrame to list of tuples for batch insert
            values = [tuple(x) for x in data.to_numpy()]
            batch_size = 1000
            
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                cur.executemany(insert_query, batch)
                
            conn.commit()
            
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# Define Airflow tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=['{{ ti.xcom_pull(task_ids="extract_data") }}'],
    provide_context=True,
    dag=dag,
)

# Add initialize database task
init_db_task = PythonOperator(
    task_id='initialize_database',
    python_callable=initialize_database,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_args=['{{ ti.xcom_pull(task_ids="transform_data") }}'],
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> init_db_task >> load_task
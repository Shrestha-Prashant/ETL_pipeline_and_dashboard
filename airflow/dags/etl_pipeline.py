from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
import psycopg2
from urllib.parse import quote_plus

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define default arguments with more robust error handling
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,  # Increased retries for better reliability
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10)
}

# Initialize DAG with 2-minute schedule
dag = DAG(
    'etl_pipeline_chronological',
    default_args=default_args,
    description='ETL pipeline for processing ecommerce data in chronological order',
    schedule_interval='*/2 * * * *',  # Runs every 2 minutes
    start_date=datetime(2024, 12, 26),
    catchup=False,
)

def get_next_file_to_process(processed_files_path, input_dir):
    """Get the next file to process in chronological order."""
    os.makedirs(processed_files_path, exist_ok=True)
    
    processed_files = set()
    processed_files_list = os.path.join(processed_files_path, 'processed_files.txt')
    if os.path.exists(processed_files_list):
        with open(processed_files_list, 'r') as f:
            processed_files = set(f.read().splitlines())
    
    # Extract number from filename and sort numerically
    available_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    available_files.sort(key=lambda x: int(x.split('_')[1].split('.')[0]))
    
    for file in available_files:
        if file not in processed_files:
            return file
    
    return None

def mark_file_as_processed(filename, processed_files_path):
    """Record processed file in tracking file"""
    processed_files_list = os.path.join(processed_files_path, 'processed_files.txt')
    with open(processed_files_list, 'a') as f:
        f.write(f"{filename}\n")

def extract_data(**context):
    """Extract data from incoming files in chronological order."""
    input_dir = '/Users/panda/Documents/ETL_pipeline_and_dashboard/incoming'
    processed_files_path = '/Users/panda/Documents/ETL_pipeline_and_dashboard/processed_tracking'
   
    # Get next file to process
    next_file = get_next_file_to_process(processed_files_path, input_dir)
   
    if not next_file:
        logger.info("No new files to process")
        return None
   
    file_to_process = os.path.join(input_dir, next_file)
    logger.info(f"Processing file: {next_file}")
   
    # Mark file as processed
    mark_file_as_processed(next_file, processed_files_path)
   
    return file_to_process

def transform_data(file_path, **context):
    """Clean and transform the data with enhanced error handling."""
    if not file_path:
        return None
       
    try:
        data = pd.read_csv(file_path)
        data['days_since_prior_order'].fillna(-1, inplace=True)  # Special value for first orders
        # data = data.dropna()
         # Drop duplicates
        data = data.drop_duplicates()
       
        # Additional data validation
        required_columns = ['order_id', 'user_id', 'order_number', 'order_dow',
                          'order_hour_of_day', 'days_since_prior_order', 'product_id',
                          'add_to_cart_order', 'reordered', 'department_id',
                          'department', 'product_name']
       
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Add features
        data['order_time_category'] = pd.cut(data['order_hour_of_day'], 
                                           bins=[0, 6, 12, 18, 24], 
                                           labels=['night', 'morning', 'afternoon', 'evening'])
    
        
        # Sort values
        data = data.sort_values(['order_id', 'add_to_cart_order'])
        
        cleaned_path = file_path.replace('incoming', 'processed')
        os.makedirs(os.path.dirname(cleaned_path), exist_ok=True)
        data.to_csv(cleaned_path, index=False)
       
        logger.info(f"Successfully transformed file: {file_path}")
        return cleaned_path
       
    except Exception as e:
        logger.error(f"Error transforming file {file_path}: {str(e)}")
        raise

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
init_db_task = PythonOperator(
    task_id='initialize_database',
    python_callable=initialize_database,
    dag=dag,
)

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

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_args=['{{ ti.xcom_pull(task_ids="transform_data") }}'],
    provide_context=True,
    dag=dag,
)

# Modified task dependencies
init_db_task >> extract_task >> transform_task >> load_task
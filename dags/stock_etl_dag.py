from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import psycopg2  # or psycopg2 for PostgreSQL

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# Remove explicit timezone handling to avoid errors.
dag = DAG(
    'stock_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for multiple stocks using yfinance',
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    start_date=datetime(2025, 4, 7),  # No timezone here
    catchup=False,
)

STOCKS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'FB', 'NFLX', 'NVDA', 'INTC', 'SPY']

def extract(**kwargs):
    stock_data = {}
    for symbol in STOCKS:
        df = yf.download(symbol, period="1d", interval="1m")
        df = df.tz_localize('UTC')  # Ensure the data is localized to UTC first
        stock_data[symbol] = df.reset_index()
    kwargs['ti'].xcom_push(key='stock_data', value=stock_data)

def transform(**kwargs):
    stock_data = kwargs['ti'].xcom_pull(key='stock_data', task_ids='extract_data')
    transformed = {}
    for symbol, df in stock_data.items():
        df['Symbol'] = symbol
        df['MovingAvg'] = df['Close'].rolling(window=5).mean()
        transformed[symbol] = df
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed)

def load(**kwargs):
    transformed = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    
    # PostgreSQL connection (using psycopg2 + SQLAlchemy)
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
    
    for symbol, df in transformed.items():
        df.to_sql(
            f'stock_{symbol}',
            engine,
            if_exists='append',
            index=False,
            method='multi'  # Batch insert for performance
        )

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load,
    provide_context=True,
    dag=dag
)

extract_task >> transform_task >> load_task

from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts/ingestion/')))

def kafka_stream_data():
    try:
        from kafka_fashion_producer import main # type: ignore
        return main()
    except Exception as e:
        logging.error(f"Error in kafka streaming: {str(e)}")
        raise

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="kafka_streaming_dag",
    description="Stream fashion data to Kafka topics",
    default_args=default_args,
    schedule_interval="*/1 * * * *",  # Every 1 minute
    catchup=False,
    max_active_runs=1
) as dag:
    
    kafka_stream_task = PythonOperator(
        task_id='kafka_stream_orders',
        python_callable=kafka_stream_data
    )
    
    kafka_stream_task
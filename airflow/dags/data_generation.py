from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
from airflow import DAG
import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts/producer/')))

def data_generation():
    try:
        from fashion_data_generator import main  # type: ignore
        return main()
    except Exception as e:
        logging.error(f"Error in data_generation: {str(e)}")
        raise

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_generation_dag",
    description="DAG for generating fashion data",
    default_args=default_args,
    schedule_interval="*/30 * * * *",   # Every 30 minutes
    catchup=False,
    max_active_runs=1
) as dag:
    
    # Create operator instances
    data_generation_task = PythonOperator(
        task_id='data_generation',
        python_callable=data_generation
    )

    # Set task dependencies
    data_generation_task
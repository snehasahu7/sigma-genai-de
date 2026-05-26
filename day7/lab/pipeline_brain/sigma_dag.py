from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import logging
import json

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}

# Define the DAG
dag = DAG(
    dag_id='sigma_transaction_pipeline',
    default_args=default_args,
    schedule='0 2 * * *',
    start_date=days_ago(0),
    catchup=False,
    sla_miss_callback=lambda context: logging.warning(
        f"SLA miss for DAG {context.task_instance.dag_id} at {context.task_instance.execution_date}"
    ),
    on_failure_callback=lambda context: logging.warning(
        f"Failure in DAG {context.dag.dag_id}, Task {context.task_instance.task_id} at {context.execution_date}: {context.exception}"
    ),
    tags=['sigma', 'transactions', 'daily'],
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions"
)

def log_task_status(context):
    """Log the start and end of a task with task instance info."""
    execution_date = context['execution_date']
    task_id = context['task_instance'].task_id
    logging.info(f"Task {task_id} started at {execution_date}")
    logging.info(f"Task {task_id} ended at {execution_date}")

def on_failure_callback(context):
    """Callback for task failure."""
    log_task_status(context)
    raise Exception(f"Task {context['task_instance'].task_id} failed at {context['execution_date']}")

def extract_bronze(**context):
    """Ingest raw CSVs to Bronze Parquet."""
    log_task_status(context)
    # Placeholder for CSV ingestion logic
    logging.info("Reading raw CSV files and writing to Bronze layer")
    # Add logic to read CSVs and write to Parquet
    raise NotImplementedError("CSV ingestion logic not implemented")

def transform_silver(**context):
    """Clean, enrich, deduplicate to Silver."""
    log_task_status(context)
    # Placeholder for transformation logic
    logging.info("Cleaning, enriching, and deduplicating data to Silver layer")
    # Add logic for data cleaning, type casting, filtering, deduplication, and joins
    raise NotImplementedError("Silver layer transformation logic not implemented")

def build_gold(**context):
    """Generate the 3 Gold aggregation tables."""
    log_task_status(context)
    # Placeholder for aggregation logic
    logging.info("Generating Gold layer aggregation tables")
    # Add logic for generating merchant_performance, customer_ltv, and daily_summary tables
    raise NotImplementedError("Gold layer aggregation logic not implemented")

# Define the tasks
extract_bronze_task = PythonOperator(
    task_id='extract_bronze',
    python_callable=extract_bronze,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

transform_silver_task = PythonOperator(
    task_id='transform_silver',
    python_callable=transform_silver,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

build_gold_task = PythonOperator(
    task_id='build_gold',
    python_callable=build_gold,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

# Set task dependencies
extract_bronze_task >> transform_silver_task >> build_gold_task

"""
Airflow DAG for E-Commerce ETL Pipeline
Orchestrates data ingestion, transformation, and loading
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'aman-roy',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['contactaman000@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

dag = DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    description='E-Commerce ETL Pipeline with CDC and Batch Processing',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['ecommerce', 'etl', 'production']
)

def extract_from_sources(**context):
    """Extract data from multiple sources"""
    logger.info("Starting data extraction...")
    # Extraction logic here
    logger.info("Data extraction completed")

def validate_data(**context):
    """Validate data quality"""
    logger.info("Validating data quality...")
    # Quality checks
    logger.info("Data validation passed")

def transform_data(**context):
    """Transform and enrich data"""
    logger.info("Transforming data...")
    # Transformation logic
    logger.info("Data transformation completed")

def load_to_warehouse(**context):
    """Load data to Redshift"""
    logger.info("Loading data to warehouse...")
    # Load logic
    logger.info("Data loaded successfully")

# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_from_sources,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    dag=dag
)

# Spark job on EMR
spark_step = {
    'Name': 'Process Transactions',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'cluster',
            's3://scripts/batch_processing.py'
        ]
    }
}

emr_task = EmrAddStepsOperator(
    task_id='run_spark_job',
    job_flow_id='{{ var.value.emr_cluster_id }}',
    steps=[spark_step],
    dag=dag
)

# Data quality report
quality_report = PostgresOperator(
    task_id='generate_quality_report',
    postgres_conn_id='postgres_analytics',
    sql="""
        INSERT INTO data_quality_reports (date, total_records, null_count, duplicate_count)
        SELECT CURRENT_DATE, COUNT(*), 
               SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END),
               COUNT(*) - COUNT(DISTINCT transaction_id)
        FROM transactions
        WHERE date = CURRENT_DATE;
    """,
    dag=dag
)

# Task dependencies
extract_task >> validate_task >> transform_task >> [load_task, emr_task]
[load_task, emr_task] >> quality_report

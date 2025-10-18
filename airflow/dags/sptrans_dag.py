

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from main_dag_runner import run_pipeline

default_args = {
    'owner': 'Wellington Santos',
    'start_date': datetime(2025, 10, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='sptrans_pipeline_dag',
    default_args=default_args,
    schedule_interval= '0 4 * * * *',  # Executa todo dia às 4h da manhã
    catchup=False,
    tags=['sptrans', 'api'],
)

executar_pipeline = PythonOperator(
    task_id='executar_main',
    python_callable=run_pipeline,
    dag=dag,
)
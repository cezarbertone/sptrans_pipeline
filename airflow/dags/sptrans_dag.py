

import sys
sys.path.append('/app')

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Wellington Santos',
    'start_date': datetime(2025, 10, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='sptrans_pipeline_dag',
    default_args=default_args,
    schedule_interval='0 4 * * *',
    catchup=False,
    tags=['sptrans', 'api']
)

def executar_pipeline():
    from pipelines.main_dag_runner import run_pipeline
    run_pipeline()

executar_pipeline_task = PythonOperator(
    task_id='executar_main',
    python_callable=executar_pipeline,
    dag=dag
)
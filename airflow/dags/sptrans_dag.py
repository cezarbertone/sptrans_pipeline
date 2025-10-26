import sys
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator

from airflow import DAG

# Adiciona o caminho do projeto ao sys.path
sys.path.append('/app/airflow')

# Argumentos padrão da DAG
default_args = {
    'owner': 'Wellington Santos',
    'start_date': datetime(2025, 10, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def executar_pipeline():
    from pipelines.main_dag_runner import run_pipeline
    run_pipeline()

# Definição da DAG com contexto
with DAG(
    dag_id='sptrans_pipeline_dag',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['sptrans', 'api']
) as dag:
    executar_pipeline_task = PythonOperator(
        task_id='executar_main',
        python_callable=executar_pipeline
    )
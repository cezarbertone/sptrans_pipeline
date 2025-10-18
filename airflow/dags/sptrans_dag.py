from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
from dotenv import load_dotenv

# Adiciona o caminho do projeto ao sys.path
sys.path.append('/app/sptrans_pipeline')

# Carrega variáveis de ambiente
load_dotenv(dotenv_path='/app/.env')

from main import main  # importa sua função principal

default_args = {
    'owner': 'Wellington Santos',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sptrans_pipeline_dag',
    default_args=default_args,
    description='Executa main.py para buscar dados da SPTrans',
    schedule_interval='@daily',
    catchup=False,
    tags=['sptrans', 'api'],
) as dag:

    executar_pipeline = PythonOperator(
        task_id='executar_main',
        python_callable=main,
    )

    executar_pipeline
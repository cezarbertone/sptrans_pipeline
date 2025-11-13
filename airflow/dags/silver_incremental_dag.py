from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from processors.incremental_posicao import atualizar_posicao_incremental
from processors.create_indexes import criar_indices

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="silver_incremental_com_indices",
    start_date=datetime(2025, 11, 12),
    schedule_interval="*/5 * * * *",  # a cada 5 minutos
    catchup=False,
    default_args=default_args,
    tags=["sptrans", "silver", "incremental"]
) as dag:

    task_incremental = PythonOperator(
        task_id="atualizar_posicao_incremental",
        python_callable=atualizar_posicao_incremental
    )

    task_create_indexes = PythonOperator(
        task_id="criar_indices_silver",
        python_callable=criar_indices
    )

    task_incremental >> task_create_indexes
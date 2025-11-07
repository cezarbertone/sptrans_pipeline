from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from processors.transform_silver import processar_silver

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="bronze_to_silver",
    start_date=datetime(2025, 11, 6),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["sptrans", "silver", "etl"]
) as dag:

    task_transform = PythonOperator(
        task_id="transformar_bronze_para_silver",
        python_callable=processar_silver,
        op_kwargs={
            "tabela_silver": "linhas_sptrans"  # Nome da tabela no Postgres
        }
    )
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from processors.transform_gold import processar_gold

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="silver_to_gold",
    start_date=datetime(2025, 11, 6),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["sptrans", "gold", "etl"]
) as dag:

    task_gold = PythonOperator(
        task_id="transformar_silver_para_gold",
        python_callable=processar_gold
    )
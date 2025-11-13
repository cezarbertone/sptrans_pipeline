from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from processors.transform_posicao_silver import processar_silver

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="posicao_bronze_to_silver",
    description="Transforma dados da camada Bronze para Silver e exporta para MinIO",
    start_date=datetime(2025, 11, 11),
    schedule_interval=None,  # será disparada pela DAG anterior
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sptrans", "silver", "minio", "postgres"]
) as dag:

    processar_silver_task = PythonOperator(
        task_id="processar_posicao_silver",
        python_callable=processar_silver,
        op_kwargs={
            "tabela_silver": "posicao_veiculos"  # Nome da tabela Silver
        }
    )
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from processors.extract_api_posicao import extrair_posicao_e_salvar_minio

# Configurações padrão
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="posicao_to_minio",
    description="Extrai posição dos veículos da API SPTrans e salva no MinIO, depois dispara DAG para carregar no Postgres",
    start_date=datetime(2025, 11, 10),
    schedule_interval="*/5 * * * *",  # a cada 5 minutos
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sptrans", "posicao", "minio"]
) as dag:

    # Task 1: Extrair posição e salvar no MinIO
    extrair_posicao_task = PythonOperator(
        task_id="extrair_posicao_e_salvar_minio",
        python_callable=extrair_posicao_e_salvar_minio
    )

    # Task 2: Disparar DAG 2 (MinIO → Postgres)
    trigger_dag2_task = TriggerDagRunOperator(
        task_id="trigger_posicao_minio_postgres",
        trigger_dag_id="posicao_minio_postgres",  # DAG 2 deve ter este dag_id
        wait_for_completion=False  # Não bloqueia a DAG 1 esperando a DAG 2 terminar
    )

    # Orquestração
    extrair_posicao_task >> trigger_dag2_task
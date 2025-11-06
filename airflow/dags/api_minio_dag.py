from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from processors.extract_api import extrair_e_salvar_minio

# Definição da DAG
with DAG(
    dag_id="api_to_minio",
    description="Extrai dados da API SPTrans e salva no MinIO (camada bronze)",
    start_date=datetime(2025, 11, 5),
    schedule_interval="0 4 * * *",  # Executa apenas manualmente
    catchup=False,
    tags=["sptrans", "api", "minio"]
) as dag:

    tarefa_extrair_e_salvar = PythonOperator(
        task_id="extrair_e_salvar_minio",
        python_callable=extrair_e_salvar_minio,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    tarefa_extrair_e_salvar

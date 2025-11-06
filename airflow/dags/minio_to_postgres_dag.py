from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from processors.load_postgres import carregar_todas_zonas

with DAG(
    dag_id='minio_to_postgres',
    description='Carrega dados do MinIO para Postgres no schema bronze',
    start_date=datetime(2025, 11, 5),
    schedule_interval='0 5 * * *',  # Todos os dias às 05:00
    catchup=False,
    tags=['sptrans', 'minio', 'postgres']
) as dag:

    carregar = PythonOperator(
        task_id='carregar_do_minio_para_postgres',
        python_callable=carregar_todas_zonas,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
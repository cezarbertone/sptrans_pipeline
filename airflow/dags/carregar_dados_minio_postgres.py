from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Função que executa o script Python
def executar_script():
    os.system("python processors/carregar_postgres.py")


# Argumentos padrão da DAG
default_args = {
    'owner': 'Wellington Santos',
    'start_date': datetime(2025, 10, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# Definição da DAG
with DAG(
    dag_id="carregar_dados_minio_postgres",
    start_date=datetime(2023, 1, 1),
    schedule_interval='*/5 * * * *',
    catchup=False,
    description="DAG para carregar dados do MinIO para PostgreSQL",
    tags=["minio", "postgres", "etl"]
) as dag:

    tarefa_carregar_postgres = PythonOperator(
        task_id="executar_carregamento",
        python_callable=executar_script
    )

    tarefa_carregar_postgres
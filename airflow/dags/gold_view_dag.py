from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from processors.create_view_gold import criar_view_e_update

# Configurações padrão
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}

# Definição da DAG
with DAG(
    dag_id="gold_view_dag",
    start_date=datetime(2025, 11, 7),
    schedule_interval="@daily",  # Ajuste conforme necessidade
    catchup=False,
    default_args=default_args,
    tags=["sptrans", "gold", "view"]
) as dag:

    # Task para criar VIEW e atualizar descrições
    task_create_view = PythonOperator(
        task_id="criar_view_gold",
        python_callable=criar_view_e_update
    )
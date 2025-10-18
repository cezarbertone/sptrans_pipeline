from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id='teste_dag_simples',
    start_date=datetime(2025, 10, 18),
    schedule_interval='@daily',
    catchup=False
) as dag:
    start = DummyOperator(task_id='start')
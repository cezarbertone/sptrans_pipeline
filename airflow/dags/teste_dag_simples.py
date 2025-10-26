from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id='teste_dag',
    start_date=datetime(2025, 10, 18),
    schedule_interval='0 4 * * *',
    catchup=False
) as dag:
    start = DummyOperator(task_id='start')

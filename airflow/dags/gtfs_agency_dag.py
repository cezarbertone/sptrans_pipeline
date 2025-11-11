from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def process_file():
    input_file = r"C:\Users\USER\Desktop\GITFIA\sptrans_pipeline\airflow\data\gtfs\agency.txt"
    output_file = r"C:\Users\USER\Desktop\GITFIA\sptrans_pipeline\airflow\data\gtfs\logs\agency.log"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    print(content)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)

with DAG(
    dag_id="gtfs_agency",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "sptrans"]
) as dag:
    process_task = PythonOperator(
        task_id="process_agency",
        python_callable=process_file
    )

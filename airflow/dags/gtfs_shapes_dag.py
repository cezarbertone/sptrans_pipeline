from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def process_file():
    input_file = r"C:\Users\USER\Desktop\GITFIA\sptrans_pipeline\airflow\data\gtfs\shapes.txt"
    output_file = r"C:\Users\USER\Desktop\GITFIA\sptrans_pipeline\airflow\data\gtfs\logs\shapes.log"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    # Note: do not load extremely large files into memory at once in production.
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            print(line.rstrip())
    # Create a copy of the file (streaming) to avoid memory issues
    with open(input_file, 'r', encoding='utf-8') as src, open(output_file, 'w', encoding='utf-8') as dst:
        for chunk in iter(lambda: src.read(1024*1024), ''):
            dst.write(chunk)

with DAG(
    dag_id="gtfs_shapes",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "sptrans"]
) as dag:
    process_task = PythonOperator(
        task_id="process_shapes",
        python_callable=process_file
    )

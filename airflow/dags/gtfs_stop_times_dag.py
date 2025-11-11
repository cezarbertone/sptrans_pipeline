from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def process_file():
    input_file = r"C:\Users\USER\Desktop\GITFIA\sptrans_pipeline\airflow\data\gtfs\stop_times.txt"
    output_file = r"C:\Users\USER\Desktop\GITFIA\sptrans_pipeline\airflow\data\gtfs\logs\stop_times.log"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    # Stream file to logs to avoid loading large files into memory
    with open(input_file, 'r', encoding='utf-8') as src, open(output_file, 'w', encoding='utf-8') as dst:
        for line in src:
            print(line.rstrip())
            dst.write(line)

with DAG(
    dag_id="gtfs_stop_times",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "sptrans"]
) as dag:
    process_task = PythonOperator(
        task_id="process_stop_times",
        python_callable=process_file
    )

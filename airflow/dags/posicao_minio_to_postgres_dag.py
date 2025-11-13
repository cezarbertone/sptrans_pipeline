from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from processors.load_posicao_postgres import carregar_arquivos_posicao
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# -----------------------------
# Configurações padrão
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["alertas@seudominio.com"],  # Altere para seu e-mail
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

# -----------------------------
# Função principal
# -----------------------------
def carregar_com_logs(**context):
    print("🚀 Iniciando carga de arquivos do MinIO para Postgres...")
    arquivos_processados = carregar_arquivos_posicao()
    print(f"✅ Processo concluído! Total de arquivos carregados: {arquivos_processados}")
    return arquivos_processados  # Retorna para XCom

# -----------------------------
# Task para exibir contagem
# -----------------------------
def exibir_contagem(**context):
    total = context['ti'].xcom_pull(task_ids='carregar_posicao_postgres')
    print(f"📊 Total de arquivos carregados nesta execução: {total}")

# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="posicao_minio_postgres",
    description="Carrega dados de posição do MinIO para Postgres com partições automáticas",
    start_date=datetime(2025, 11, 10),
    schedule_interval="0 * * * *",  # roda todo início de hora
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sptrans", "posicao", "minio", "postgres"]
) as dag:

    carregar_posicao_task = PythonOperator(
        task_id="carregar_posicao_postgres",
        python_callable=carregar_com_logs
    )

    exibir_task = PythonOperator(
        task_id="exibir_contagem",
        python_callable=exibir_contagem
    )


    # ✅ Nova Task: Disparar DAG 3
    trigger_dag3_task = TriggerDagRunOperator(
        task_id="trigger_posicao_bronze_to_silver",
        trigger_dag_id="posicao_bronze_to_silver",
        wait_for_completion=False  # não bloqueia a DAG 2
    )

    # Orquestração
    carregar_posicao_task >> exibir_task >> trigger_dag3_task

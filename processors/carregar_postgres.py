import os
import pandas as pd
import psycopg2
from minio import Minio

def carregar_do_minio_para_postgres():
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=False
    )

    bucket_name = "sptrans-data"
    objeto = "bronze/linhas_zona_sul.csv"
    caminho_local = "/tmp/linhas_zona_sul.csv"

    print("📥 Baixando arquivo do MinIO...")
    client.fget_object(bucket_name, objeto, caminho_local)
    print(f"📁 Arquivo salvo em: {caminho_local}")

    df = pd.read_csv(caminho_local)

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS linhas_zona_sul (
            cl INTEGER PRIMARY KEY,
            lc BOOLEAN,
            lt TEXT,
            sl INTEGER,
            tp TEXT
        )
    """)
    conn.commit()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO linhas_zona_sul (cl, lc, lt, sl, tp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (cl) DO NOTHING
        """, (row['cl'], row['lc'], row['lt'], row['sl'], row['tp']))
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Dados carregados do MinIO para o PostgreSQL com sucesso!")

import requests
import pandas as pd
import os
import psycopg2
from minio import Minio
from api.autenticacao import autenticar

def salvar_no_postgres(df):
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
    print("✅ Dados salvos no PostgreSQL com sucesso!")

def salvar_no_minio(df):
    caminho_csv = "/app/data/linhas_zona_sul.csv"
    os.makedirs(os.path.dirname(caminho_csv), exist_ok=True)
    df.to_csv(caminho_csv, index=False)

    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    bucket_name = "sptrans-data"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    client.fput_object(bucket_name, "linhas_zona_sul.csv", caminho_csv)
    print("✅ Arquivo CSV enviado para o MinIO com sucesso!")

def buscar_linhas_zona_sul():
    session = autenticar()
    if not session:
        print("❌ Sessão inválida.")
        return

    termos_zona_sul = [
        "Santo Amaro", "Capão Redondo", "Campo Limpo", "Jardim Ângela",
        "Jardim São Luís", "Socorro", "Interlagos", "Cidade Dutra",
        "Grajaú", "Parelheiros", "Vila das Belezas", "Vila Andrade", "Morumbi",
        "Terminal Santo Amaro", "Terminal Capelinha", "Terminal João Dias",
        "Terminal Parelheiros", "Terminal Grajaú", "Terminal Varginha"
    ]

    dfs = []

    for termo in termos_zona_sul:
        url = f"https://api.olhovivo.sptrans.com.br/v2.1/Linha/Buscar?termosBusca={termo}"
        response = session.get(url)

        if response.status_code == 200:
            linhas = response.json()
            if linhas:
                df = pd.DataFrame(linhas)
                dfs.append(df)
        else:
            print(f"⚠️ Erro ao buscar linhas com termo '{termo}': {response.status_code}")

    if dfs:
        df_completo = pd.concat(dfs, ignore_index=True)
        df_completo.drop_duplicates(subset="cl", inplace=True)
        salvar_no_postgres(df_completo)
        salvar_no_minio(df_completo)
    else:
        print("⚠️ Nenhuma linha encontrada para os termos da Zona Sul.")

# Executar
buscar_linhas_zona_sul()

import os
import pandas as pd
import psycopg2
from minio import Minio

from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

def carregar_todas_zonas():
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=False
    )

    bucket_name = "sptrans-data"
    zonas = ["central", "leste", "oeste", "norte", "sul"]

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()

    # Criar esquema bronze se não existir
    cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    conn.commit()

    for zona in zonas:
        objeto = f"bronze/linhas_zona_{zona}.csv"
       # caminho_local = f"/tmp/linhas_zona_{zona}.csv"
        caminho_local = f"/opt/airflow/data/linhas_zona_{zona}.csv"
        client.fget_object(bucket_name, objeto, caminho_local)

        if not os.path.isfile(caminho_local):
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho_local}")
        tabela = f"bronze.linhas_zona_{zona}"

        print(f"\n📥 Baixando arquivo para zona {zona}...")
        client.fget_object(bucket_name, objeto, caminho_local)
        print(f"📁 Arquivo salvo em: {caminho_local}")

        df = pd.read_csv(caminho_local)
        expected_cols = ['cl','lc','lt','sl','tl','tp','ts']
        if len(df.columns) != len(expected_cols):
            df.columns = expected_cols

        df['lc'] = df['lc'].astype(bool)
        df['sl'] = df['sl'].astype(int)
        df['tl'] = df['tl'].astype(int)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {tabela} (
                cl INTEGER PRIMARY KEY,
                lc BOOLEAN,
                lt TEXT,
                sl INTEGER,
                tl INTEGER,
                tp TEXT,
                ts TEXT
            )
        """)
        conn.commit()

        for _, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO {tabela} (cl, lc, lt, sl, tl, tp, ts)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cl) DO NOTHING
            """, (
                row['cl'], row['lc'], row['lt'], row['sl'],
                row['tl'], row['tp'], row['ts']
            ))
        conn.commit()
        print(f"✅ Dados da zona {zona} carregados no esquema bronze!")

    cursor.close()
    conn.close()
    print("\n🎯 Todas as zonas foram carregadas no esquema bronze com sucesso!")

if __name__ == "__main__":
    carregar_todas_zonas()
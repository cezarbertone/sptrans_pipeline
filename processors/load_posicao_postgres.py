import os
import psycopg2
import pandas as pd
from minio import Minio
from io import BytesIO
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configurações MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET = "sptrans-data"

# Configurações Postgres
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def criar_particao_if_not_exists(cursor, ano, mes):
    inicio = f"{ano}-{mes:02d}-01"
    fim = f"{ano+1}-01-01" if mes == 12 else f"{ano}-{mes+1:02d}-01"
    nome_particao = f"bronze.posicao_veiculos_{ano}_{mes:02d}"

    cursor.execute(f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'posicao_veiculos_{ano}_{mes:02d}'
        ) THEN
            EXECUTE 'CREATE TABLE {nome_particao} PARTITION OF bronze.posicao_veiculos
                     FOR VALUES FROM (''{inicio}'') TO (''{fim}'');';
        END IF;
    END$$;
    """)


def carregar_arquivos_posicao():
    # Conexão MinIO
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

    # Conexão Postgres
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()

    # Garantir esquema e tabelas
    cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bronze.posicao_veiculos (
        codigo_linha TEXT,
        sl INTEGER,
        py DOUBLE PRECISION,
        px DOUBLE PRECISION,
        hr TIMESTAMP,
        ta TIMESTAMP,
        v DOUBLE PRECISION
    ) PARTITION BY RANGE (hr);
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bronze.posicao_arquivos_processados (
        arquivo TEXT PRIMARY KEY,
        data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()

    # Contador de arquivos novos
    arquivos_novos = 0

    # Listar arquivos no MinIO
    objetos = client.list_objects(BUCKET, prefix="bronze/posicao/", recursive=True)
    for obj in objetos:
        if obj.object_name.endswith(".parquet"):
            # Verificar se já foi processado
            cursor.execute("SELECT 1 FROM bronze.posicao_arquivos_processados WHERE arquivo = %s", (obj.object_name,))
            if cursor.fetchone():
                print(f"⏩ Arquivo já processado, ignorando: {obj.object_name}")
                continue

            print(f"➡️ Processando arquivo: {obj.object_name}")
            response = client.get_object(BUCKET, obj.object_name)
            parquet_data = BytesIO(response.read())
            df = pd.read_parquet(parquet_data)

            if df.empty:
                print("⚠️ Arquivo vazio, ignorando.")
                continue

            # Ajustar tipos de data
            df['hr'] = pd.to_datetime(df['hr'])
            df['ta'] = pd.to_datetime(df['ta'])

            # Criar partição se necessário
            ano = df['hr'].dt.year.iloc[0]
            mes = df['hr'].dt.month.iloc[0]
            criar_particao_if_not_exists(cursor, ano, mes)

            # Inserir dados usando COPY
            buffer = BytesIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cursor.copy_expert("""
                COPY bronze.posicao_veiculos (codigo_linha, sl, py, px, hr, ta, v)
                FROM STDIN WITH CSV
            """, buffer)
            conn.commit()

            # Registrar arquivo como processado
            cursor.execute("INSERT INTO bronze.posicao_arquivos_processados (arquivo) VALUES (%s)", (obj.object_name,))
            conn.commit()

            arquivos_novos += 1
            print(f"✅ Dados inseridos e arquivo registrado: {obj.object_name}")

    cursor.close()
    conn.close()
    print(f"🎯 Todos os arquivos novos foram carregados com sucesso! Total: {arquivos_novos}")
    return arquivos_novos


if __name__ == "__main__":
    total = carregar_arquivos_posicao()
    print(f"Arquivos processados nesta execução: {total}")

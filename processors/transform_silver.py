import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import unicodedata
from minio import Minio
import io

# -----------------------------
# Carregar variáveis do .env
# -----------------------------
load_dotenv()

POSTGRES_CONN = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

BUCKET = "sptrans-data"
PREFIX_SILVER = "silver"

client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

# -----------------------------
# Função para garantir bucket
# -----------------------------
def garantir_bucket():
    if not client.bucket_exists(BUCKET):
        print(f"Bucket '{BUCKET}' não existe. Criando...")
        client.make_bucket(BUCKET)
    else:
        print(f"Bucket '{BUCKET}' já existe.")

# -----------------------------
# Função para ler dados do Postgres com UNION ALL
# -----------------------------
def ler_dados_postgres():
    print("Lendo dados das tabelas Bronze...")
    conn_str = f"postgresql://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['dbname']}"
    engine = create_engine(conn_str)

    query = """
    SELECT cl, lc, lt, sl, tl, tp, ts, 'ZONA_SUL' AS zona FROM bronze.linhas_zona_sul
    UNION ALL
    SELECT cl, lc, lt, sl, tl, tp, ts, 'ZONA_LESTE' AS zona FROM bronze.linhas_zona_leste
    UNION ALL
    SELECT cl, lc, lt, sl, tl, tp, ts, 'ZONA_NORTE' AS zona FROM bronze.linhas_zona_norte
    UNION ALL
    SELECT cl, lc, lt, sl, tl, tp, ts, 'ZONA_OESTE' AS zona FROM bronze.linhas_zona_oeste
    UNION ALL
    SELECT cl, lc, lt, sl, tl, tp, ts, 'ZONA_CENTRAL' AS zona FROM bronze.linhas_zona_central;
    """
    return pd.read_sql(query, engine)

# -----------------------------
# Função de limpeza e ajustes
# -----------------------------
def limpar_dados(df):
    print("Iniciando limpeza e padronização...")
    df.columns = ['cl', 'lc', 'lt', 'sl', 'tl', 'tp', 'ts', 'zona']

    # Conversão de tipos
    df['cl'] = df['cl'].astype(str)
    df['lc'] = df['lc'].astype(bool)
    df['lt'] = df['lt'].astype(str)
    df['sl'] = df['sl'].astype(int)
    df['tl'] = df['tl'].astype(int)
    df['tp'] = df['tp'].astype(str)
    df['ts'] = df['ts'].astype(str)
    df['zona'] = df['zona'].astype(str)

    # Normalização de texto
    for col in ['lt', 'tp', 'ts', 'zona']:
        df[col] = df[col].str.strip().str.upper()
        df[col] = df[col].apply(lambda x: unicodedata.normalize('NFKD', x))

    # Ajuste nos valores
    df['lc'] = df['lc'].apply(lambda x: 'Sim' if x else 'Nao')
    df['sl'] = df['sl'].apply(lambda x: 'Principal' if x == 1 else 'Secundario')

    # Remover duplicatas
    df.drop_duplicates(inplace=True)

    # Renomear colunas para nomes finais
    df.rename(columns={
        'cl': 'CodigoLinha',
        'lc': 'LinhaCircular',
        'lt': 'NumeroLetreiro',
        'sl': 'Sentido',
        'tl': 'TipoLinha',
        'tp': 'DescricaoPrincipal',
        'ts': 'DescricaoSecundario',
        'zona': 'Zona'
    }, inplace=True)

    print("Limpeza concluída e colunas renomeadas.")
    return df

# -----------------------------
# Funções de salvamento
# -----------------------------
def salvar_silver_minio(df, arquivo):
    garantir_bucket()
    caminho = f"{PREFIX_SILVER}/{arquivo}"
    print(f"Salvando dados no MinIO (Silver): {caminho}")
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
    parquet_buffer.seek(0)
    client.put_object(BUCKET, caminho, parquet_buffer, len(parquet_buffer.getvalue()))

def salvar_silver_postgres(df, tabela):
    print(f"Salvando dados no Postgres (Silver): silver.{tabela}")
    conn_str = f"postgresql://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['dbname']}"
    engine = create_engine(conn_str)
    df.to_sql(tabela, engine, schema="silver", if_exists="replace", index=False)

# -----------------------------
# Função principal
# -----------------------------
def processar_silver(tabela_silver="linhas_sptrans"):
    df = ler_dados_postgres()
    df = limpar_dados(df)
    salvar_silver_minio(df, f"{tabela_silver}.parquet")
    salvar_silver_postgres(df, tabela_silver)
    print("✅ Processamento Silver concluído.")

if __name__ == "__main__":
    processar_silver()
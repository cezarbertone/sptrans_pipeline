import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
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
# Ler dados da Bronze
# -----------------------------
def ler_dados_postgres():
    print("Lendo dados da tabela Bronze posicao_veiculos...")
    conn_str = f"postgresql://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['dbname']}"
    engine = create_engine(conn_str)

    query = """
    SELECT codigo_linha, sl, py, px, hr, ta, v FROM bronze.posicao_veiculos;
    """
    return pd.read_sql(query, engine)

# -----------------------------
# Limpeza e ajustes
# -----------------------------
def limpar_dados(df):
    print("Iniciando limpeza e padronização...")
    df.columns = ['codigo_linha', 'sl', 'py', 'px', 'hr', 'ta', 'v']

    # Conversão de tipos
    df['codigo_linha'] = df['codigo_linha'].astype(str).str.strip().str.upper()
    df['sl'] = df['sl'].astype(int)
    df['py'] = df['py'].astype(float)
    df['px'] = df['px'].astype(float)
    df['hr'] = pd.to_datetime(df['hr'])
    df['ta'] = pd.to_datetime(df['ta'])
    df['v'] = df['v'].astype(str)

    # Criar coluna para particionamento
    df['data_particao'] = df['hr'].dt.date

    # Remover duplicatas
    df.drop_duplicates(inplace=True)

    # Renomear colunas finais
    df.rename(columns={
        'codigo_linha': 'CodigoLinha',
        'sl': 'Sentido',
        'py': 'Latitude',
        'px': 'Longitude',
        'hr': 'HorarioRegistro',
        'ta': 'HorarioAtualizacao',
        'v': 'Veiculo'
    }, inplace=True)

    print("Limpeza concluída e colunas renomeadas.")
    return df

# -----------------------------
# Salvar no MinIO com particionamento
# -----------------------------
def salvar_silver_minio(df, tabela):
    garantir_bucket()
    for data in df['data_particao'].unique():
        df_part = df[df['data_particao'] == data]
        caminho = f"{PREFIX_SILVER}/{tabela}/data_particao={data}/{tabela}.parquet"
        print(f"Salvando partição no MinIO: {caminho}")
        parquet_buffer = io.BytesIO()
        df_part.to_parquet(parquet_buffer, engine="pyarrow", index=False)
        parquet_buffer.seek(0)
        client.put_object(BUCKET, caminho, parquet_buffer, len(parquet_buffer.getvalue()))

# -----------------------------
# Salvar no Postgres com coluna de partição
# -----------------------------
def salvar_silver_postgres(df, tabela):
    print(f"Salvando dados no Postgres (Silver): silver.{tabela}")
    conn_str = f"postgresql://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['dbname']}"
    engine = create_engine(conn_str)
    df.to_sql(tabela, engine, schema="silver", if_exists="append", index=False)

# -----------------------------
# Função principal
# -----------------------------
def processar_silver(tabela_silver="posicao_veiculos"):
    df = ler_dados_postgres()
    df = limpar_dados(df)
    salvar_silver_minio(df, tabela_silver)
    salvar_silver_postgres(df, tabela_silver)
    print("✅ Processamento Silver concluído com particionamento.")

if __name__ == "__main__":
    processar_silver()

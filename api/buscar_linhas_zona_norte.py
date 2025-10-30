from dotenv import load_dotenv
load_dotenv()

import requests
import pandas as pd
import os
from minio import Minio
from api.autenticacao import autenticar



def salvar_no_minio(df):
    print("🚀 Iniciando salvamento no MinIO...")
    try:
        caminho_csv = "/app/data/linhas_zona_norte.csv"
        os.makedirs(os.path.dirname(caminho_csv), exist_ok=True)
        df.to_csv(caminho_csv, index=False)
        print(f"📁 CSV salvo em: {caminho_csv}")

        client = Minio(
            os.getenv("MINIO_ENDPOINT", "minio:9000"),
            access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            secure=False
        )
        print("🔐 Cliente MinIO criado.")

        bucket_name = "sptrans-data"
        if not client.bucket_exists(bucket_name):
            print(f"🪣 Bucket '{bucket_name}' não existe. Criando...")
            client.make_bucket(bucket_name)
        else:
            print(f"🪣 Bucket '{bucket_name}' já existe.")

        destino = "bronze/linhas_zona_norte.csv"
        print(f"📤 Enviando arquivo para MinIO em: {destino}")
        client.fput_object(bucket_name, destino, caminho_csv)
        print(f"✅ Arquivo enviado para {destino} no bucket '{bucket_name}'")

    except Exception as e:
        print(f"❌ Erro ao salvar no MinIO: {type(e).__name__} - {e}")
        raise

def buscar_linhas_zona_norte():
    session = autenticar()
    if not session:
        print("❌ Sessão inválida.")
        return

    termos_zona_norte = [
        "Santana", "Tucuruvi", "Mandaqui", "Casa Verde", "Limão",
        "Vila Maria", "Vila Guilherme", "Jaçanã", "Tremembé",
        "Parada Inglesa", "Lauzane Paulista", "Jardim São Paulo",
        "Imirim", "Horto Florestal",
        "Terminal Santana", "Terminal Cachoeirinha", "Terminal Vila Nova Cachoeirinha"
    ]

    dfs = []

    for termo in termos_zona_norte:
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
        salvar_no_minio(df_completo)
    else:
        print("⚠️ Nenhuma linha encontrada para os termos da Zona Norte.")

# Executar
buscar_linhas_zona_norte()
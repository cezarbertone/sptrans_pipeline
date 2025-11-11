import os
import pandas as pd
import requests
from minio import Minio
from dotenv import load_dotenv
from api.autenticacao import autenticar

load_dotenv()

# Função para salvar no MinIO e localmente
def salvar_no_minio(df, nome_arquivo):
    try:
        caminho_csv = f"/opt/airflow/data/{nome_arquivo}.csv"
        os.makedirs(os.path.dirname(caminho_csv), exist_ok=True)

        # Salvar localmente
        df.to_csv(caminho_csv, index=False)
        print(f"📁 CSV salvo em: {caminho_csv}")

        # Conexão com MinIO
        client = Minio(
            os.getenv("MINIO_ENDPOINT", "minio:9000"),
            access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            secure=False
        )

        bucket_name = "sptrans-data"
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"✅ Bucket '{bucket_name}' criado no MinIO")

        destino = f"bronze/{nome_arquivo}.csv"
        client.fput_object(bucket_name, destino, caminho_csv)
        print(f"✅ Arquivo enviado para {destino} no bucket '{bucket_name}'")

    except Exception as e:
        print(f"❌ Erro ao salvar no MinIO: {e}")


# Função genérica para buscar e salvar sem tratamento
def buscar_e_salvar(session, termos, nome_arquivo):
    dfs = []
    for termo in termos:
        url = f"https://api.olhovivo.sptrans.com.br/v2.1/Linha/Buscar?termosBusca={termo}"
        print(f"➡️ Consultando: {url}")
        response = session.get(url)
        if response.status_code == 200:
            linhas = response.json()
            if linhas:
                dfs.append(pd.DataFrame(linhas))
        else:
            print(f"❌ Erro HTTP {response.status_code} para termo '{termo}'")

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        salvar_no_minio(df_final, nome_arquivo)
        print(f"✅ Total registros: {len(df_final)}")
    else:
        print(f"⚠️ Nenhum dado encontrado para {nome_arquivo}")


# Funções específicas para cada zona
def buscar_linhas_zona_sul(session):
    termos = ["Santo Amaro", "Capão Redondo", "Campo Limpo", "Socorro", "Interlagos"]
    buscar_e_salvar(session, termos, "linhas_zona_sul")

def buscar_linhas_zona_leste(session):
    termos = ["Itaquera", "São Mateus", "Tatuapé", "Penha", "Guaianases"]
    buscar_e_salvar(session, termos, "linhas_zona_leste")

def buscar_linhas_zona_norte(session):
    termos = ["Santana", "Tremembé", "Casa Verde", "Vila Maria", "Jaçanã"]
    buscar_e_salvar(session, termos, "linhas_zona_norte")

def buscar_linhas_zona_oeste(session):
    termos = ["Pinheiros", "Butantã", "Lapa", "Jaguaré", "Perdizes"]
    buscar_e_salvar(session, termos, "linhas_zona_oeste")

def buscar_linhas_zona_central(session):
    termos = ["Sé", "República", "Consolação", "Liberdade", "Bela Vista"]
    buscar_e_salvar(session, termos, "linhas_zona_central")


# Função principal
def extrair_e_salvar_minio():
    session = autenticar()
    if not session:
        print("❌ Sessão inválida.")
        return

    print("✅ Iniciando extração de todas as zonas...")
    buscar_linhas_zona_sul(session)
    buscar_linhas_zona_leste(session)
    buscar_linhas_zona_norte(session)
    buscar_linhas_zona_oeste(session)
    buscar_linhas_zona_central(session)
    print("🎯 Processo concluído!")
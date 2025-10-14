import requests
import pandas as pd
import os
from sqlalchemy import create_engine
from api.autenticacao import autenticar

def buscar_linhas_zona_sul_e_salvar_postgres():
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

        # Conexão com PostgreSQL usando variáveis de ambiente
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "sptrans")
        db_user = os.getenv("DB_USER", "postgres")
        db_password = os.getenv("DB_PASSWORD", "postgres")

        engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

        # Salvar no banco
        df_completo.to_sql("linhas_zona_sul", engine, if_exists="replace", index=False)
        print("✅ Dados salvos no PostgreSQL com sucesso!")
    else:
        print("⚠️ Nenhuma linha encontrada para os termos da Zona Sul.")

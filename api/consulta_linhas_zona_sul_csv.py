import requests
import pandas as pd
import os
from api.autenticacao import autenticar

def buscar_linhas_zona_sul(salvar_em="data/linhas_zona_sul.csv"):
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
        os.makedirs("data", exist_ok=True)
        df_completo.to_csv(salvar_em, index=False)
        print(f"💾 Dados salvos em '{salvar_em}'")
    else:
        print("⚠️ Nenhuma linha encontrada para os termos da Zona Sul.")

# Executar
buscar_linhas_zona_sul()
import requests
import pandas as pd
from api.autenticacao import autenticar

def buscar_linhas_intervalo(inicio=80, fim=89, salvar_em="data/linhas_80a89.csv"):
    session = autenticar()
    if not session:
        print("❌ Sessão inválida.")
        return

    dfs = []

    for prefixo in range(inicio, fim + 1):
        termo_busca = str(prefixo)
        url_busca_linha = f"https://api.olhovivo.sptrans.com.br/v2.1/Linha/Buscar?termosBusca={termo_busca}"
        response = session.get(url_busca_linha)

        if response.status_code == 200:
            linhas = response.json()
            if linhas:
                df = pd.DataFrame(linhas)
                dfs.append(df)
        else:
            print(f"❌ Erro ao buscar linhas com prefixo {prefixo}: {response.status_code}")

    if dfs:
        df_completo = pd.concat(dfs, ignore_index=True)
        df_completo.drop_duplicates(subset="cl", inplace=True)
        df_completo.to_csv(salvar_em, index=False)
        print(f"💾 Dados salvos em '{salvar_em}'")
    else:
        print("⚠️ Nenhuma linha encontrada com prefixos informados.")
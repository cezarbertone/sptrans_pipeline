import requests
import pandas as pd
import os
from api.autenticacao import autenticar

def buscar_previsao_chegada(caminho_csv_linhas="data/linhas_80a89.csv", salvar_em="data/previsao_chegada.csv"):
    session = autenticar()
    if not session:
        print("❌ Sessão inválida.")
        return

    if not os.path.exists(caminho_csv_linhas):
        print(f"❌ Arquivo '{caminho_csv_linhas}' não encontrado.")
        return

    df_linhas = pd.read_csv(caminho_csv_linhas)
    codigos_linhas = df_linhas["cl"].drop_duplicates().tolist()

    previsoes = []

    for cl in codigos_linhas:
        url_previsao = f"https://api.olhovivo.sptrans.com.br/v2.1/Previsao/Linha?codigoLinha={cl}"
        response = session.get(url_previsao)

        if response.status_code == 200:
            dados = response.json()
            if "p" in dados:
                for parada in dados["p"]:
                    cod_parada = parada.get("cp")
                    nome_parada = parada.get("np")
                    for veiculo in parada.get("vs", []):
                        previsoes.append({
                            "codigo_linha": cl,
                            "codigo_parada": cod_parada,
                            "nome_parada": nome_parada,
                            "prefixo_veiculo": veiculo.get("p"),
                            "horario_previsto": veiculo.get("t"),
                            "latitude": veiculo.get("py"),
                            "longitude": veiculo.get("px")
                        })
        else:
            print(f"⚠️ Erro ao buscar previsão da linha {cl}: {response.status_code}")

    if previsoes:
        os.makedirs("data", exist_ok=True)
        df_previsoes = pd.DataFrame(previsoes)
        df_previsoes.to_csv(salvar_em, index=False)
        print(f"💾 Previsões salvas em '{salvar_em}'")
    else:
        print("⚠️ Nenhuma previsão encontrada.")

# Executar
buscar_previsao_chegada()
def buscar_linha(session, termo_busca):
    url = f"http://api.olhovivo.sptrans.com.br/v2.1/Linha/Buscar?termosBusca={termo_busca}"
    response = session.get(url)

    if response.status_code == 200:
        linhas = response.json()
        for linha in linhas:
            print(f"🔹 Código: {linha['cl']} | Letreiro: {linha['lt']} | Sentido: {linha['sl']}")
    else:
        print("Erro ao buscar linha:", response.status_code)

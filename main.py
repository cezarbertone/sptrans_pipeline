
from api.autenticacao import autenticar
from api.consulta_varias_linhas import buscar_linhas_intervalo
from api.previsao_chegada import buscar_previsao_chegada
from api.consulta_linhas_zona_sul_csv import buscar_linhas_zona_sul


def main():
    session = autenticar()
    if session:
        buscar_linhas_intervalo()
        buscar_previsao_chegada()
        buscar_linhas_zona_sul()

if __name__ == "__main__":
    main()

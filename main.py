

from dotenv import load_dotenv
load_dotenv()


from api.autenticacao import autenticar
from api.consulta_linhas_zona_sul import buscar_linhas_zona_sul
#from api.consulta_linhas_zona_sul_csv import buscar_linhas_zona_sul

def main():
    session = autenticar()
    if session:
  #     buscar_linhas_intervalo()
   #    buscar_previsao_chegada()
        buscar_linhas_zona_sul()

if __name__ == "__main__":
    main()
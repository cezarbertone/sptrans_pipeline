from dotenv import load_dotenv
load_dotenv()

from api.autenticacao import autenticar

from api.buscar_linhas_zona_sul import buscar_linhas_zona_sul
from processors.carregar_postgres import carregar_do_minio_para_postgres

from api.buscar_linhas_zona_leste import buscar_linhas_zona_leste
from processors.carregar_postgres_leste import carregar_do_minio_para_postgres as carregar_postgres_leste

from api.buscar_linhas_zona_norte import buscar_linhas_zona_norte
from processors.carregar_postgres_norte import carregar_do_minio_para_postgres as carregar_postgres_norte

from api.buscar_linhas_zona_oeste import buscar_linhas_zona_oeste
from processors.carregar_postgres_oeste import carregar_do_minio_para_postgres as carregar_postgres_oeste

from api.buscar_linhas_zona_central import buscar_linhas_zona_central
from processors.carregar_postgres_centro import carregar_do_minio_para_postgres as carregar_postgres_centro

import time

def main():
    try:
        session = autenticar()
        if session:
            # Zona sul
            buscar_linhas_zona_sul()
            carregar_do_minio_para_postgres()

            #Zona Leste
            buscar_linhas_zona_leste()
            carregar_postgres_leste()

            #Zona Norte
            buscar_linhas_zona_norte()
            carregar_postgres_norte()

            #Zona Oeste
            buscar_linhas_zona_oeste()
            carregar_postgres_oeste()

            #Zona Central
            buscar_linhas_zona_central()
            carregar_postgres_centro()

    except Exception as e:
        print(f"❌ Erro durante execução: {e}")


if __name__ == "__main__":
    main()
from dotenv import load_dotenv
load_dotenv()

from api.autenticacao import autenticar
from api.buscar_linhas_zona_sul import buscar_linhas_zona_sul
from processors.carregar_postgres import carregar_do_minio_para_postgres

from api.buscar_linhas_zona_leste import buscar_linhas_zona_leste
from processors.carregar_postgres_leste import carregar_do_minio_para_postgres as carregar_postgres_leste

import time

def main():
    try:
        session = autenticar()
        if session:
            # Zona sul
            buscar_linhas_zona_sul()
            carregar_do_minio_para_postgres()

            # Zona Leste
            buscar_linhas_zona_leste()
            carregar_postgres_leste()

    except Exception as e:
        print(f"❌ Erro durante execução: {e}")


if __name__ == "__main__":
    main()
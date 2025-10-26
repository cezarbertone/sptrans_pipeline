from dotenv import load_dotenv
load_dotenv()

from api.autenticacao import autenticar
from api.buscar_linhas_sptrans_com_minio import buscar_linhas_zona_sul

import time

def main():
    try:
        session = autenticar()
        if session:
            buscar_linhas_zona_sul()
    except Exception as e:
        print(f"❌ Erro durante execução: {e}")


if __name__ == "__main__":
    main()
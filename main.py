from dotenv import load_dotenv
load_dotenv()

from api.autenticacao import autenticar
from api.buscar_linhas_sptrans_com_minio import buscar_linhas_zona_sul

import time

def main():
    session = autenticar()
    if session:
        buscar_linhas_zona_sul()

    while True:
        print("⏳ Container ativo e aguardando...")
        time.sleep(60)

if __name__ == "__main__":
    main()
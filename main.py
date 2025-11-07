
import os

import traceback
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Importações dos módulos internos
from api.autenticacao import autenticar
from api.buscar_linhas import (
    buscar_linhas_zona_sul,
    buscar_linhas_zona_leste,
    buscar_linhas_zona_norte,
    buscar_linhas_zona_oeste,
    buscar_linhas_zona_central
)
from processors.load_postgres import carregar_todas_zonas


def main():
    try:
        # Autenticação
        session = autenticar()
        if session:
            print("✅ Autenticação realizada com sucesso!")

            # Buscar dados de todas as zonas usando a sessão autenticada
            buscar_linhas_zona_sul(session)
            buscar_linhas_zona_leste(session)
            buscar_linhas_zona_norte(session)
            buscar_linhas_zona_oeste(session)
            buscar_linhas_zona_central(session)

            # Carregar todas as zonas no Postgres
            carregar_todas_zonas()
        else:
            print("❌ Autenticação falhou.")
    except Exception as e:
        print(f"❌ Erro durante execução: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
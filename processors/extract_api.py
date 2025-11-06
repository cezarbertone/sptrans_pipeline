from api.autenticacao import autenticar
from api.buscar_linhas import (
    buscar_linhas_zona_sul,
    buscar_linhas_zona_leste,
    buscar_linhas_zona_norte,
    buscar_linhas_zona_oeste,
    buscar_linhas_zona_central
)

def extrair_e_salvar_minio():
    session = autenticar()
    if not session:
        raise Exception("Falha na autenticação")
    print("✅ Autenticação realizada com sucesso!")

    buscar_linhas_zona_sul(session)
    buscar_linhas_zona_leste(session)
    buscar_linhas_zona_norte(session)
    buscar_linhas_zona_oeste(session)
    buscar_linhas_zona_central(session)

    print("✅ Dados extraídos e enviados para MinIO (camada bronze)")
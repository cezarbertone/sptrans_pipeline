from processors.extract_api import extrair_e_salvar_minio
from processors.load_postgres2 import carregar_do_minio_para_postgres

def main():
    try:
        extrair_e_salvar_minio()
        carregar_do_minio_para_postgres()
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    main()
``
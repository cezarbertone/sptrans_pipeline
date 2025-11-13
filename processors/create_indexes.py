import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

POSTGRES_CONN = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

def get_engine():
    conn_str = f"postgresql://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}@" \
               f"{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['dbname']}"
    return create_engine(conn_str)

def criar_indices():
    engine = get_engine()
    with engine.begin() as conn:
        print("🔹 Verificando/criando índices na tabela silver.posicao_veiculos_enriquecida...")

        indices = [
            """CREATE INDEX IF NOT EXISTS idx_enriquecida_horario 
               ON silver.posicao_veiculos_enriquecida("HorarioRegistro");""",
            """CREATE INDEX IF NOT EXISTS idx_enriquecida_data 
               ON silver.posicao_veiculos_enriquecida("data_particao");""",
            """CREATE INDEX IF NOT EXISTS idx_enriquecida_codigo_linha 
               ON silver.posicao_veiculos_enriquecida("CodigoLinha");""",
            """CREATE INDEX IF NOT EXISTS idx_enriquecida_sentido 
               ON silver.posicao_veiculos_enriquecida("Sentido");"""
            """CREATE INDEX IF NOT EXISTS idx_fato_dia_hora_minuto
               ON gold.fato_posicao_veiculos("DiaSemana", "Hora", "Minuto");"""

        ]

        for idx in indices:
            conn.execute(text(idx))

        print("✅ Índices criados/verificados com sucesso!")

if __name__ == "__main__":
    criar_indices()
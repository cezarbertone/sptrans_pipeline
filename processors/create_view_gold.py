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

def criar_view_e_update():
    engine = get_engine()
    with engine.begin() as conn:
        print("🔹 Atualizando descrições nas dimensões...")

        # Atualizar descrição das zonas
        conn.execute(text("""
        UPDATE gold.dim_zona
        SET descricao = CASE nome_zona
            WHEN 'ZONA_SUL' THEN 'Região Sul'
            WHEN 'ZONA_LESTE' THEN 'Região Leste'
            WHEN 'ZONA_NORTE' THEN 'Região Norte'
            WHEN 'ZONA_OESTE' THEN 'Região Oeste'
            WHEN 'ZONA_CENTRAL' THEN 'Região Central'
        END;
        """))

        # Atualizar descrição dos tipos de linha
        conn.execute(text("""
        UPDATE gold.dim_tipo_linha
        SET descricao_tipo = CASE codigo_tipo
            WHEN 10 THEN 'Radial'
            WHEN 11 THEN 'Noturna'
            WHEN 21 THEN 'Interbairros'
            WHEN 22 THEN 'Perimetral'
            WHEN 23 THEN 'Circular'
            WHEN 24 THEN 'Especial'
            WHEN 25 THEN 'Troncal'
            WHEN 31 THEN 'Semi-Expressa'
            WHEN 41 THEN 'Expressa'
            WHEN 42 THEN 'Corredor'
        END;
        """))

        print("✅ Dimensões atualizadas.")

        print("🔹 Criando VIEW consolidada para BI...")
        conn.execute(text("""
        CREATE OR REPLACE VIEW gold.vw_dashboard_sptrans AS
        SELECT
            dz.nome_zona,
            dz.descricao AS descricao_zona,
            fz.total_linhas,
            fz.perc_circulares,
            dt.codigo_tipo,
            dt.descricao_tipo,
            ft.total_linhas AS total_por_tipo,
            fs.total AS total_sentido,
            ds.descricao AS sentido,
            fo.descricao_origem,
            fo.descricao_destino,
            fo.total_ocorrencias
        FROM gold.fact_linhas_por_zona fz
        JOIN gold.dim_zona dz ON dz.zona_id = fz.zona_id
        LEFT JOIN gold.fact_linhas_por_tipo ft ON TRUE
        LEFT JOIN gold.dim_tipo_linha dt ON dt.tipo_id = ft.tipo_id
        LEFT JOIN gold.fact_sentido_por_zona fs ON fs.zona_id = dz.zona_id
        LEFT JOIN gold.dim_sentido ds ON ds.sentido_id = fs.sentido_id
        LEFT JOIN gold.fact_top_origem_destino fo ON TRUE;
        """))

        print("✅ VIEW criada com sucesso: gold.vw_dashboard_sptrans")

if __name__ == "__main__":
    criar_view_e_update()
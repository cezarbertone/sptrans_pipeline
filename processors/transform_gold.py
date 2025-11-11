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

# Queries para popular dimensões (com aspas para colunas maiúsculas)
QUERIES_DIM = [
    """
    INSERT INTO gold.dim_zona (nome_zona)
    SELECT DISTINCT "Zona" FROM silver.linhas_sptrans
    ON CONFLICT (nome_zona) DO NOTHING;
    """,
    """
    INSERT INTO gold.dim_sentido (descricao)
    SELECT DISTINCT "Sentido" FROM silver.linhas_sptrans
    ON CONFLICT (descricao) DO NOTHING;
    """,
    """
    INSERT INTO gold.dim_tipo_linha (codigo_tipo)
    SELECT DISTINCT "TipoLinha" FROM silver.linhas_sptrans
    ON CONFLICT (codigo_tipo) DO NOTHING;
    """
]

# Queries para popular fatos (com aspas para colunas maiúsculas)
QUERIES_FACT = [
    # Linhas por Zona
    """
    INSERT INTO gold.fact_linhas_por_zona (zona_id, total_linhas, perc_circulares)
    SELECT dz.zona_id,
           COUNT(DISTINCT s."CodigoLinha") AS total_linhas,
           ROUND(SUM(CASE WHEN s."LinhaCircular" = 'Sim' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_circulares
    FROM silver.linhas_sptrans s
    JOIN gold.dim_zona dz ON dz.nome_zona = s."Zona"
    GROUP BY dz.zona_id;
    """,
    # Linhas por Tipo
    """
    INSERT INTO gold.fact_linhas_por_tipo (tipo_id, total_linhas)
    SELECT dt.tipo_id,
           COUNT(*) AS total_linhas
    FROM silver.linhas_sptrans s
    JOIN gold.dim_tipo_linha dt ON dt.codigo_tipo = s."TipoLinha"
    GROUP BY dt.tipo_id;
    """,
    # Sentido por Zona
    """
    INSERT INTO gold.fact_sentido_por_zona (zona_id, sentido_id, total)
    SELECT dz.zona_id,
           ds.sentido_id,
           COUNT(*) AS total
    FROM silver.linhas_sptrans s
    JOIN gold.dim_zona dz ON dz.nome_zona = s."Zona"
    JOIN gold.dim_sentido ds ON ds.descricao = s."Sentido"
    GROUP BY dz.zona_id, ds.sentido_id;
    """,
    # Top Origem-Destino
    """
    INSERT INTO gold.fact_top_origem_destino (descricao_origem, descricao_destino, total_ocorrencias)
    SELECT "DescricaoPrincipal", "DescricaoSecundario", COUNT(*) AS total_ocorrencias
    FROM silver.linhas_sptrans
    GROUP BY "DescricaoPrincipal", "DescricaoSecundario"
    ORDER BY total_ocorrencias DESC
    LIMIT 50;
    """
]

def processar_gold():
    engine = get_engine()
    with engine.begin() as conn:
        print("🔹 Populando dimensões...")
        for query in QUERIES_DIM:
            conn.execute(text(query))
        print("✅ Dimensões populadas.")

        print("🔹 Limpando fatos antes de inserir...")
        conn.execute(text("""
            TRUNCATE gold.fact_linhas_por_zona,
                     gold.fact_linhas_por_tipo,
                     gold.fact_sentido_por_zona,
                     gold.fact_top_origem_destino;
        """))

        print("🔹 Populando fatos...")
        for query in QUERIES_FACT:
            conn.execute(text(query))
        print("✅ Fatos populados com sucesso.")

if __name__ == "__main__":
    processar_gold()
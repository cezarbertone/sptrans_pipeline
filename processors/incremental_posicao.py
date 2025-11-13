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

def atualizar_posicao_incremental():
    engine = get_engine()
    with engine.begin() as conn:
        print("🔹 Executando carga incremental para posicao_veiculos_enriquecida...")

        query = """
        INSERT INTO silver.posicao_veiculos_enriquecida (
                "CodigoLinha",
                "DescricaoCompleto",
                "Sentido",
                "LinhaCircular",
                "Latitude",
                "Longitude",
                "HorarioRegistro",
                "HorarioAtualizacao",
                "PeriodoDia",
                "DiaSemana",
                "Hora",
                "Minuto",
                "data_particao"
        )
        SELECT
                pv."CodigoLinha",
                ls."LetreiroCompleto" || ' - ' || ls."DescricaoPrincipal" || ' / ' || ls."DescricaoSecundario" AS "DescricaoCompleto",
            CASE 
                WHEN pv."Sentido" = 1 THEN 'Ida' 
                WHEN pv."Sentido" = 2 THEN 'Volta' 
            ELSE ''  END AS  "Sentido",
                ls."LinhaCircular",
                pv."Latitude",
                pv."Longitude",
                pv."HorarioRegistro",
                pv."HorarioAtualizacao",
            CASE
            WHEN EXTRACT(HOUR FROM pv."HorarioAtualizacao") BETWEEN 0 AND 5 THEN 'Madrugada'
            WHEN EXTRACT(HOUR FROM pv."HorarioAtualizacao") BETWEEN 6 AND 11 THEN 'Manhã'
            WHEN EXTRACT(HOUR FROM pv."HorarioAtualizacao") BETWEEN 12 AND 17 THEN 'Tarde' ELSE 'Noite' end as "PeriodoDia", 

        CASE EXTRACT(DOW FROM CAST(data_particao AS DATE))
            WHEN 0 THEN 'Domingo'
            WHEN 1 THEN 'Segunda'
            WHEN 2 THEN 'Terça'
            WHEN 3 THEN 'Quarta'
            WHEN 4 THEN 'Quinta'
            WHEN 5 THEN 'Sexta'
            WHEN 6 THEN 'Sabado' END AS "DiaSemana",
            EXTRACT(HOUR FROM pv."HorarioAtualizacao") AS "Hora",
            EXTRACT(MINUTE FROM pv."HorarioAtualizacao") AS "Minuto",
            pv."data_particao"
            
FROM silver.posicao_veiculos pv
JOIN silver.linhas_sptrans ls
    ON pv."CodigoLinha" = ls."LetreiroCompleto"
   AND pv."Sentido" = ls."Sentido" 
WHERE pv."HorarioRegistro" > (
    SELECT COALESCE(MAX("HorarioRegistro"), '1900-01-01') FROM silver.posicao_veiculos_enriquecida
);
        """

        conn.execute(text(query))
        print("✅ Incremental concluído com sucesso!")

if __name__ == "__main__":
    atualizar_posicao_incremental()
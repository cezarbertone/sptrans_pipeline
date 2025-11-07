

-- 1. Linhas por Zona
SELECT
    nome_zona AS "Zona",
    SUM(total_linhas) AS "Total de Linhas"
FROM gold.vw_dashboard_sptrans
GROUP BY nome_zona
ORDER BY "Total de Linhas" DESC;

-- 2. Percentual de Circulares

SELECT
    nome_zona AS "Zona",
    AVG(perc_circulares) AS "Percentual Circulares"
FROM gold.vw_dashboard_sptrans
GROUP BY nome_zona
ORDER BY "Percentual Circulares" DESC


--3. Top Origem-Destino

SELECT
    descricao_origem AS "Origem",
    descricao_destino AS "Destino",
    SUM(total_ocorrencias) AS "Ocorrências"
FROM gold.vw_dashboard_sptrans
GROUP BY descricao_origem, descricao_destino
ORDER BY "Ocorrências" DESC
LIMIT 10;

--4. Sentido por Zona
SELECT
    nome_zona AS "Zona",
    tipo_sentido AS "Sentido",
    SUM(total_sentido) AS "Total"
FROM gold.vw_dashboard_sptrans
GROUP BY nome_zona, tipo_sentido
ORDER BY nome_zona, tipo_sentido;

--5. Linhas por Tipo
SELECT
    tipo_sentido AS "Tipo de Linha",
    SUM(total_por_tipo) AS "Total"
FROM gold.vw_dashboard_sptrans
GROUP BY tipo_sentido
ORDER BY "Total" DESC;
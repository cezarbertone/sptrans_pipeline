


SELECT
    cl AS codigo_linha,
    lc AS localizacao,
    sl AS sentido_linha,
    lt AS itinerario,
    tp AS tipo
FROM {{ ref('linhas_zona_sul_bronze') }}


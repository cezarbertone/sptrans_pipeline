
  
    

  create  table "sptrans"."public_silver"."linhas__dbt_tmp"
  
  
    as
  
  (
    SELECT
    cl AS codigo_linha,
    lc AS localizacao,
    sl AS sentido_linha,
    lt AS itinerario,
    tp AS tipo
FROM "sptrans"."public_bronze"."linhas_zona_sul_bronze"
  );
  
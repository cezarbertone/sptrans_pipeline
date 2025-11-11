
  create view "sptrans"."public_bronze"."linhas_zona_sul_bronze__dbt_tmp"
    
    
  as (
    SELECT *
FROM public.linhas_zona_sul
  );
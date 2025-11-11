select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select codigo_linha
from "sptrans"."public"."linhas"
where codigo_linha is null



      
    ) dbt_internal_test
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select localizacao
from "sptrans"."public"."linhas"
where localizacao is null



      
    ) dbt_internal_test
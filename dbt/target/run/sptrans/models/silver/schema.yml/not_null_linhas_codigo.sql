select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select codigo
from "sptrans"."public"."linhas"
where codigo is null



      
    ) dbt_internal_test
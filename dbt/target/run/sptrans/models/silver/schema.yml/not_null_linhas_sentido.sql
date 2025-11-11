select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select sentido
from "sptrans"."public"."linhas"
where sentido is null



      
    ) dbt_internal_test
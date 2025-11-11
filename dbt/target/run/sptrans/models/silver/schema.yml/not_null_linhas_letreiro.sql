select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select letreiro
from "sptrans"."public"."linhas"
where letreiro is null



      
    ) dbt_internal_test
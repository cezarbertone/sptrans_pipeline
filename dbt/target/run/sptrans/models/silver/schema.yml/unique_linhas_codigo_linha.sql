select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    codigo_linha as unique_field,
    count(*) as n_records

from "sptrans"."public"."linhas"
where codigo_linha is not null
group by codigo_linha
having count(*) > 1



      
    ) dbt_internal_test
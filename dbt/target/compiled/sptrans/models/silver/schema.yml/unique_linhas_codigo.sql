
    
    

select
    codigo as unique_field,
    count(*) as n_records

from "sptrans"."public"."linhas"
where codigo is not null
group by codigo
having count(*) > 1



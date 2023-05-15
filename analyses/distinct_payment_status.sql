with payments as (select * from {{ref('stg_payment')}})

select distinct status from payments
with payment as (
    select * from {{ref('stg_payment')}}
)

select 
    order_id,
    sum(amount_in_dollars) as total_amount
from payment
group by 1
having total_amount < 0
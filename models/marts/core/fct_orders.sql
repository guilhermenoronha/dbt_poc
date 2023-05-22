with

orders as (select * from {{ref('stg_orders')}}),
payment as (select * from {{ref('stg_payment')}}),

order_payments as (
    select
        order_id,
        sum(case when payment_status = 'success' then amount_in_dollars end) as amount_in_dollars

    from payment
    group by 1
),

final as (
    select 
        O.order_id,
        O.customer_id,
        O.order_date,
        coalesce(order_payments.amount_in_dollars, 0) as amount_in_dollars
    from orders O
    left join order_payments using (order_id)
)

select * from final
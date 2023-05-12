with source as (
    select
        id as payment_id,
        orderid as order_id,
        amount / 100 as amount_in_dollars,
        status,
        created as created_at
    from {{source('stripe', 'payment')}}
)

select * from source
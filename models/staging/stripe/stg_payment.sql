with source as (
    select
        id as payment_id,
        paymentmethod as payment_method,
        orderid as order_id,
        {{ cents_to_dollars('amount') }} as amount_in_dollars,
        status,
        created as created_at
    from {{source('stripe', 'payment')}}
)

select * from source
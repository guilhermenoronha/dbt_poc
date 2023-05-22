with orders as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        row_number() over (partition by user_id order by order_date, id) as user_order_seq,
        case when status not in ('returned','return_pending') then order_date end as non_returned_order_date,    
        status as order_status        

    from {{source('jaffle_shop', 'orders')}}

)

select * from orders
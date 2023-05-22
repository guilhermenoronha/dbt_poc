with 
    orders      as (select * from {{ ref('stg_orders') }}),
    customers   as (select * from {{ ref('stg_customers') }}),
    payment     as (select * from {{ ref('stg_payment') }} where payment_status != 'fail'),

    customer_order_history as (
        select
            a.order_id,
            a.order_status,
            b.customer_id,
            b.full_name,
            b.surname,
            b.givenname,
            c.amount_in_dollars,
            c.payment_status,
            min(a.order_date) as first_order_date,
            min(a.non_returned_order_date) as first_non_returned_order_date,
            max(a.non_returned_order_date) as most_recent_non_returned_order_date,
            COALESCE(max(user_order_seq),0) as order_count,
            count(nvl2(a.non_returned_order_date, 1 ,0)) as non_returned_order_count,
            sum(nvl2(a.non_returned_order_date, c.amount_in_dollars, 0)) as total_lifetime_value,
            sum(nvl2(a.non_returned_order_date, c.amount_in_dollars, 0))/NULLIF(count(non_returned_order_date),0) as avg_non_returned_order_value,
            SPLIT_TO_ARRAY(LISTAGG(distinct cast(a.order_id as varchar), ','), ',') as order_ids

        from orders a
        join customers b using(customer_id)
        left join payment c using(order_id)
        group by 1,2,3,4,5,6,7,8        
    )

    select * from customer_order_history
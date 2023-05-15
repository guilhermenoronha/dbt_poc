with
    payment as (select * from {{ref('stg_payment')}}),

    final as (
        select sum(amount_in_dollars) as total_revenue
        from payment
        where status = 'success'
    )

    select * from final
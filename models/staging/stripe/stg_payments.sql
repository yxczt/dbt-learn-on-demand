with 

payments as (
    select * from {{ source('dbt_zselo', 'payments') }}
),

transformed as (
    select
        ORDERID as order_id,
        max(CREATED) as payment_finalized_date,
        sum(AMOUNT) / 100.0 as total_amount_paid
    from payments
    where STATUS <> 'fail'
    group by 1
)

select * from transformed
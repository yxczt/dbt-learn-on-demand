with 

orders as (
    select ID as order_id,
        USER_ID	as customer_id,
        ORDER_DATE AS order_placed_at,
        STATUS AS order_status
    from {{ source('dbt_zselo', 'orders') }}
)

select * from orders
with 

orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

paid_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,  
        orders.order_status,
        payments.total_amount_paid,
        payments.payment_finalized_date
    from orders
        left join payments using (order_id)
)

select * from paid_orders
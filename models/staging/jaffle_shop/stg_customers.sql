with 

customers as (
    select * from {{ source('dbt_zselo', 'customers') }}
),

transformed as (
    select
        customers.ID as customer_id,
        customers.FIRST_NAME as customer_first_name,
        customers.LAST_NAME as customer_last_name
    from customers
)

select * from transformed
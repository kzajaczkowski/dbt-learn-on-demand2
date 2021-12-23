with

orders as (

    select *
    from {{ source('jaffle_shop', 'orders') }}

),

transformed as (

    select
        orders.id as order_id,
        row_number() over (order by orders.id) as transaction_seq,
        row_number() over (partition by orders.user_id order by orders.id)
            as customer_sales_seq,
        orders.user_id	as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status
    from orders
)

select *
from transformed
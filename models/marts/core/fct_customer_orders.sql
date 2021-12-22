with

-- import CTE's
orders as (

    select *
    from {{ ref('stg_jaffle_shop__orders') }}

),

customers as (

    select *
    from {{ ref('stg_jaffle_shop__customers') }}

),

payment as (

    select *
    from {{ ref('stg_stripe__payment') }}

),

-- logical CTE's

successful_payments as (

    select orderid as order_id,
        max(created) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid
    from payment
    where status <> 'fail'
    group by 1

),

paid_orders as (
    
    select  orders.order_id,
            orders.customer_id,
            orders.order_placed_at,
            orders.order_status,
            p.total_amount_paid,
            p.payment_finalized_date,
            c.customer_first_name,
            c.customer_last_name
    from orders
    left join successful_payments p
    using (order_id)
    left join customers c
    using (customer_id)

),

customer_orders as (

    select c.customer_id,
        min(order_placed_at) as fdos,
        max(order_placed_at) as most_recent_order_date,
        count(orders.order_id) as number_of_orders
    from customers c 
    left join orders
    using (customer_id)
    group by 1

),

customer_lifetime_value as (
    select
        p.order_id,
        sum(orders_atd.total_amount_paid) as customer_lifetime_value
    from paid_orders p
    left join paid_orders orders_atd
    on p.customer_id = orders_atd.customer_id
    and p.order_id >= orders_atd.order_id
    group by 1
    order by p.order_id

),

final as (

    select
        p.*,
        row_number() over (order by p.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by p.order_id)
            as customer_sales_seq,
        case when c.fdos = p.order_placed_at
            then 'new'
            else 'return' end as nvsr,
        x.customer_lifetime_value,
        c.fdos
    from paid_orders p
    left join customer_orders c
    using (customer_id)
    left outer join customer_lifetime_value x
    on x.order_id = p.order_id

)

select *
from final
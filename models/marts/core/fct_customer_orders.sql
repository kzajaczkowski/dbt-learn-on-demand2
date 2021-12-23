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
order_totals as (

    select order_id,
        max(created) as payment_finalized_date,
        sum(amount) as total_amount_paid
    from payment
    where status <> 'fail'
    group by 1

),

order_values_joined as (
    
    select  orders.*,
            p.total_amount_paid,
            p.payment_finalized_date,
            c.customer_first_name,
            c.customer_last_name--,
            -- min(orders.order_placed_at) over (partition by customer_id) as first_customer_order,
            -- max(orders.order_placed_at) over (partition by customer_id) as last_customer_order,
            -- count(orders.order_id) over (partition by customer_id) as number_of_orders,
            -- case when first_customer_order = orders.order_placed_at
            --     then 'new'
            --     else 'return' end as nvsr

    from orders
    left join order_totals p
    using (order_id)
    left join customers c
    using (customer_id)

),

customer_orders as (

    select c.customer_id,
        min(order_placed_at) as first_customer_order,
        max(order_placed_at) as last_customer_order,
        count(orders.order_id) as number_of_orders
    from customers c 
    left join orders
    using (customer_id)
    group by 1

),

final as (

    select
        p.*,
        case when c.first_customer_order = p.order_placed_at
            then 'new'
            else 'return' end as nvsr,
        sum(p.total_amount_paid) over (partition by p.customer_id order by order_placed_at) as customer_lifetime_value,
        c.first_customer_order as fdos
    from order_values_joined p
    left join customer_orders c
    using (customer_id)
)

select *
from final
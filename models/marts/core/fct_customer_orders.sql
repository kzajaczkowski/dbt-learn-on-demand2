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

    select payment.order_id,
        max(payment.created) as payment_finalized_date,
        sum(payment.amount) as total_amount_paid
    from payment
    where status <> 'fail'
    group by 1

),

order_values_joined as (
    
    select  orders.*,
            order_totals.total_amount_paid,
            order_totals.payment_finalized_date,
            customers.customer_first_name,
            customers.customer_last_name
    from orders
    left join order_totals
    using (order_id)
    left join customers
    using (customer_id)

),

customer_orders as (

    select customers.customer_id,
        min(orders.order_placed_at) as first_customer_order,
        max(orders.order_placed_at) as last_customer_order,
        count(orders.order_id) as number_of_orders
    from customers
    left join orders
    using (customer_id)
    group by 1

),

final as (

    select
        order_values_joined.*,
        case when customer_orders.first_customer_order = order_values_joined.order_placed_at
            then 'new'
            else 'return' end as nvsr,
        sum(order_values_joined.total_amount_paid) over
            (partition by order_values_joined.customer_id order by order_placed_at)
            as customer_lifetime_value,
        customer_orders.first_customer_order as fdos
    from order_values_joined
    left join customer_orders
    using (customer_id)
)

select *
from final
with jaffle_shop_customers as (

    select * from {{ ref('stg_customers') }}

),

legacy_customers as (

    select * from {{ ref('stg_legacy_customers') }}

),

orders as (

    select * from {{ ref('fct_orders') }}

),

unioned_customers as (

    select * from jaffle_shop_customers
    
    union all

    select * from legacy_customers

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(amount) as lifetime_value

    from orders

    group by 1

),

final as (

    select
        {{ dbt_utils.surrogate_key(['customer_id']) }} as customer_sk,
        unioned_customers.customer_id,
        unioned_customers.first_name,
        unioned_customers.last_name,
        unioned_customers.source_system,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_orders.lifetime_value, 0) as lifetime_value,
        unioned_customers.is_special_customer

    from unioned_customers

    left join customer_orders using (customer_id)

)

select * from final
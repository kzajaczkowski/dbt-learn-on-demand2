with payments as (

    select * from {{ ref('stg_payments')}}  

),

orders as (

    select * from {{ ref('stg_orders') }}

),

successful_payments as (

    select *
    from payments
    where status = 'success'
)

select {{ dbt_utils.surrogate_key(['customer_id']) }} as customer_sk
   , orders.customer_id
   , orders.order_id
   , orders.order_date
   , successful_payments.amount
from orders
left join successful_payments
using (order_id)
with

payment as (

    select *
    from {{ source('stripe', 'payment') }}

),

transformed as (

    select 
        orderid as order_id,
        amount / 100 as amount,
        status,
        paymentmethod as payment_method,
        created
    from payment

)

select *
from transformed
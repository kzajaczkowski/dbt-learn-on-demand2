with

payment as (

    select *
    from {{ source('stripe', 'payment') }}

),

transformed as (

    select *
    from payment

)

select *
from transformed
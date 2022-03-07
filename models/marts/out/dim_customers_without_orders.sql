with customers as (

    select *
    from {{ ref('dim_customers') }}

),

final as (

    select *
    from customers
    where number_of_orders = 0

)

select *
from final
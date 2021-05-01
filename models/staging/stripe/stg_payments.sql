select orderid as order_id
   , amount / 100 as amount
   , status

from {{ source('stripe', 'payment') }}
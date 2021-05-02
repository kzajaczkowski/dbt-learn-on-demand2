select orderid as order_id
   , amount / 100 as amount
   , status
   , paymentmethod as payment_method
from {{ source('stripe', 'payment') }}
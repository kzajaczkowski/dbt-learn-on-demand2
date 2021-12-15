select
    id as customer_id,
    {{ standardise_string('first_name') }} as first_name,
    last_name

from {{ source('jaffle_shop', 'customers') }}
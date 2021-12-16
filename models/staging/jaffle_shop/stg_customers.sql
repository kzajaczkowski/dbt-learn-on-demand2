{{ config(materialized='table') }}

select
    id as customer_id,
    {{ standardise_string('first_name') }} as first_name,
    last_name,

    1 AS NewColumn1

from {{ source('jaffle_shop', 'customers') }}
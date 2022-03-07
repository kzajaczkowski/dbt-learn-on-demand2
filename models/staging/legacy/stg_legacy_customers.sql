select
    row_number() over (order by (select 1)) as customer_id,
    {{ standardise_string('first_name') }} as first_name,
    last_name,

    1 AS NewColumn1,

    'legacy' as source_system,
    CASE WHEN FIRST_NAME = 'Anne' AND LAST_NAME = 'Turner' THEN 'Y' ELSE 'N' END as is_special_customer

from {{ ref('legacy_customers') }}


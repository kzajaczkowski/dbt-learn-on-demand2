select
    row_number() over (order by (select 1)) as customer_id,
    {{ standardise_string('first_name') }} as first_name,
    last_name,

    1 AS NewColumn1,

    'legacy' as source_system

from {{ ref('legacy_customers') }}


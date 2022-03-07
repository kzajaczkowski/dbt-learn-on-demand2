SELECT *
FROM {{ ref('dim_customers_with_orders') }}
WHERE IS_SPECIAL_CUSTOMER != 'Y'
AND SOURCE_SYSTEM = 'legacy'
AND FIRST_NAME = 'Anne'
And last_name = 'Turner'
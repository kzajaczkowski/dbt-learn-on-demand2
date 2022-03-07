SELECT *
FROM {{ ref('dim_customers_with_orders') }}
WHERE IS_SPECIAL_CUSTOMER != 'N'
AND SOURCE_SYSTEM = 'legacy'
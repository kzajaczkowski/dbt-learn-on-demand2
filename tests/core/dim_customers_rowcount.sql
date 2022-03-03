SELECT CUSTOMER_ID
FROM {{ ref('dim_customers') }}

EXCEPT

SELECT ID
FROM {{ source('jaffle_shop', 'customers') }}

UNION ALL

SELECT ID
FROM {{ source('jaffle_shop', 'customers') }}

EXCEPT

SELECT CUSTOMER_ID
FROM {{ ref('dim_customers') }}


{% snapshot customer_snapshot %}

{{
    config(
        target_schema='MARTS',
        unique_key='CUSTOMER_ID',
        strategy='check',
        check_cols=['EMAIL', 'SHIPPING_ADDRESS', 'PHONE'],
    )
}}

SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}

-- dbt adds: DBT_SCD_ID, DBT_UPDATED_AT,
--           DBT_VALID_FROM, DBT_VALID_TO
--
-- When a customer changes their address:
-- Old row: DBT_VALID_TO = change timestamp
-- New row: DBT_VALID_TO = NULL (current)
--
-- FACT_ORDERS joins using BETWEEN DBT_VALID_FROM AND DBT_VALID_TO
-- to get the address at time of order, not today's address

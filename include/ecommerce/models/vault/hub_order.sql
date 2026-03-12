{{ config(materialized='incremental', unique_key='ORDER_HK') }}

WITH source AS (
    SELECT
        MD5(ORDER_ID)          AS ORDER_HK,
        ORDER_ID               AS ORDER_BK,
        CURRENT_TIMESTAMP()    AS LOAD_DATE,
        'ORDERS_API'           AS RECORD_SOURCE
    FROM {{ ref('stg_orders') }}
)
SELECT * FROM source
{% if is_incremental() %}
WHERE ORDER_HK NOT IN (SELECT ORDER_HK FROM {{ this }})
{% endif %}
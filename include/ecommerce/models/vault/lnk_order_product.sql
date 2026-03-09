-- Link: many-to-many between orders and products
{{
    config(materialized='incremental',
           unique_key='ORDER_PRODUCT_HK')
}}
SELECT
    MD5(CONCAT(
        MD5(o.ORDER_ID),
        MD5(CAST(p.PRODUCT_ID AS VARCHAR))
    ))                                  AS ORDER_PRODUCT_HK,
    MD5(o.ORDER_ID)                     AS ORDER_HK,
    MD5(CAST(p.PRODUCT_ID AS VARCHAR))  AS PRODUCT_HK,
    CURRENT_TIMESTAMP()                 AS LOAD_DATE,
    'ORDERS_API'                        AS RECORD_SOURCE
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_products') }} p
    ON o.PRODUCT_ID = p.PRODUCT_ID
{% if is_incremental() %}
WHERE ORDER_PRODUCT_HK NOT IN (SELECT ORDER_PRODUCT_HK FROM {{ this }})
{% endif %}

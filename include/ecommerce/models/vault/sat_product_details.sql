-- Satellite: descriptive attributes with full history
{{
    config(materialized='incremental',
           unique_key='PRODUCT_HK')
}}
SELECT
    MD5(CAST(PRODUCT_ID AS VARCHAR))   AS PRODUCT_HK,
    PRODUCT_NAME, BRAND, CATEGORY,
    PRICE, STOCK_QUANTITY, RATING,
    CURRENT_TIMESTAMP()                AS LOAD_DATE,
    NULL                               AS END_DATE,
    'DUMMYJSON_API'                    AS RECORD_SOURCE,
    MD5(CONCAT(PRODUCT_NAME, CAST(PRICE AS VARCHAR),
               CAST(STOCK_QUANTITY AS VARCHAR))) AS HASH_DIFF
FROM {{ ref('stg_products') }}
{% if is_incremental() %}
WHERE HASH_DIFF NOT IN (
    SELECT HASH_DIFF FROM {{ this }}
    WHERE END_DATE IS NULL
)
{% endif %}

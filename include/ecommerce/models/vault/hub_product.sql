-- Hub: business key only, one row per product
{{
    config(materialized='incremental',
           unique_key='PRODUCT_HK')
}}
WITH source AS (
    SELECT
        MD5(CAST(PRODUCT_ID AS VARCHAR)) AS PRODUCT_HK,
        PRODUCT_ID                       AS PRODUCT_BK,
        CURRENT_TIMESTAMP()              AS LOAD_DATE,
        'DUMMYJSON_API'                  AS RECORD_SOURCE
    FROM {{ ref('stg_products') }}
)
SELECT * FROM source
{% if is_incremental() %}
    WHERE PRODUCT_HK NOT IN (SELECT PRODUCT_HK FROM {{ this }})
{% endif %}

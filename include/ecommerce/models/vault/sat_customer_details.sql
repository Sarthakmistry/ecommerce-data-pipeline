{{
    config(
        materialized='incremental',
        unique_key='CUSTOMER_HK'
    )
}}

WITH source AS (
    SELECT
        MD5(CAST(CUSTOMER_ID AS VARCHAR))   AS CUSTOMER_HK,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE,
        SHIPPING_ADDRESS,
        CITY,
        STATE,
        COUNTRY,
        CURRENT_TIMESTAMP()                 AS LOAD_DATE,
        NULL::TIMESTAMP                     AS END_DATE,
        'DUMMYJSON_API'                     AS RECORD_SOURCE,
        MD5(CONCAT(
            COALESCE(EMAIL, ''),
            COALESCE(PHONE, ''),
            COALESCE(SHIPPING_ADDRESS, ''),
            COALESCE(CITY, ''),
            COALESCE(STATE, '')
        ))                                  AS HASH_DIFF
    FROM {{ ref('stg_customers') }}
)

SELECT * FROM source
{% if is_incremental() %}
WHERE HASH_DIFF NOT IN (
    SELECT HASH_DIFF FROM {{ this }}
    WHERE END_DATE IS NULL
)
{% endif %}
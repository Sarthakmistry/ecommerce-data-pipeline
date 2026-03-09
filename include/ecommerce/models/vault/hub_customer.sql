{{
    config(
        materialized='incremental',
        unique_key='CUSTOMER_HK'
    )
}}

WITH source AS (
    SELECT
        MD5(CAST(CUSTOMER_ID AS VARCHAR))   AS CUSTOMER_HK,
        CUSTOMER_ID                         AS CUSTOMER_BK,
        CURRENT_TIMESTAMP()                 AS LOAD_DATE,
        'DUMMYJSON_API'                     AS RECORD_SOURCE
    FROM {{ ref('stg_customers') }}
)

SELECT * FROM source
{% if is_incremental() %}
    WHERE CUSTOMER_HK NOT IN (SELECT CUSTOMER_HK FROM {{ this }})
{% endif %}
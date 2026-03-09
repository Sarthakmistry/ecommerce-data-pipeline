WITH source AS (
    SELECT
        CUSTOMER_ID,
        RAW_DATA:firstName::VARCHAR     AS first_name,
        RAW_DATA:lastName::VARCHAR      AS last_name,
        RAW_DATA:email::VARCHAR         AS email,
        RAW_DATA:phone::VARCHAR         AS phone,
        RAW_DATA:address:address::VARCHAR AS shipping_address,
        RAW_DATA:address:city::VARCHAR  AS city,
        RAW_DATA:address:state::VARCHAR AS state,
        RAW_DATA:address:country::VARCHAR AS country,
        INGESTED_AT
    FROM {{ source('raw', 'RAW_CUSTOMERS') }}
),
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY CUSTOMER_ID ORDER BY INGESTED_AT DESC
        ) AS rn
    FROM source
)
SELECT * EXCLUDE (rn) FROM deduplicated WHERE rn = 1
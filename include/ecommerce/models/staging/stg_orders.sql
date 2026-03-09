WITH source AS (
    SELECT
        RAW_DATA:order_id::VARCHAR          AS order_id,
        RAW_DATA:customer_id::INT           AS customer_id,
        RAW_DATA:product_id::INT            AS product_id,
        RAW_DATA:status::VARCHAR            AS status,
        RAW_DATA:shipping_address::VARCHAR  AS shipping_address,
        RAW_DATA:order_date::DATE           AS order_date,
        RAW_DATA:total_amount::DECIMAL(10,2) AS total_amount,
        INGESTED_AT
    FROM {{ source('raw', 'RAW_ORDERS') }}
),
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY order_id ORDER BY INGESTED_AT DESC
        ) AS rn
    FROM source
)
SELECT * EXCLUDE (rn) FROM deduplicated WHERE rn = 1
WITH source AS (
    SELECT
        RAW_DATA:return_id::VARCHAR         AS return_id,
        RAW_DATA:order_id::VARCHAR          AS order_id,
        RAW_DATA:customer_id::INT           AS customer_id,
        RAW_DATA:reason::VARCHAR            AS return_reason,
        RAW_DATA:return_date::DATE          AS return_date,
        RAW_DATA:refund_amount::DECIMAL(10,2) AS refund_amount,
        INGESTED_AT
    FROM {{ source('raw', 'RAW_RETURNS') }}
),
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY return_id ORDER BY INGESTED_AT DESC
        ) AS rn
    FROM source
)
SELECT * EXCLUDE (rn) FROM deduplicated WHERE rn = 1
WITH source AS (
    SELECT
        PRODUCT_ID,
        RAW_DATA:title::VARCHAR         AS product_name,
        RAW_DATA:brand::VARCHAR         AS brand,
        RAW_DATA:category::VARCHAR      AS category,
        RAW_DATA:price::DECIMAL(10,2)   AS price,
        RAW_DATA:stock::INT             AS stock_quantity,
        RAW_DATA:description::VARCHAR   AS description,
        RAW_DATA:rating::DECIMAL(3,2)   AS rating,
        INGESTED_AT
    FROM {{ source('raw', 'RAW_PRODUCTS') }}
),
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY PRODUCT_ID ORDER BY INGESTED_AT DESC
        ) AS rn
    FROM source
)
SELECT * EXCLUDE (rn) FROM deduplicated WHERE rn = 1

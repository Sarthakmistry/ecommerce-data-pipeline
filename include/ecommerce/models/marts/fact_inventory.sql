-- models/marts/fact_inventory.sql
-- NOTE: This is a snapshot-style periodic fact table.
-- Each row represents a point-in-time stock level for a product at a warehouse.
WITH source AS (
    SELECT
        SNAPSHOT_ID,
        RAW_DATA:product_id::INT            AS PRODUCT_ID,
        RAW_DATA:stock_level::INT           AS STOCK_LEVEL,
        RAW_DATA:snapshot_date::DATE        AS SNAPSHOT_DATE,
        RAW_DATA:warehouse_location::VARCHAR AS WAREHOUSE_LOCATION,
        INGESTED_AT
    FROM {{ source('raw', 'RAW_INVENTORY') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY SNAPSHOT_ID ORDER BY INGESTED_AT DESC
        ) AS rn
    FROM source
)

SELECT
    d.SNAPSHOT_ID,
    d.SNAPSHOT_DATE,
    d.WAREHOUSE_LOCATION,
    d.STOCK_LEVEL,

    -- Derived metrics
    CASE WHEN d.STOCK_LEVEL = 0    THEN 1 ELSE 0 END AS IS_OUT_OF_STOCK,
    CASE WHEN d.STOCK_LEVEL < 20   THEN 1 ELSE 0 END AS IS_LOW_STOCK,
    CASE WHEN d.STOCK_LEVEL >= 20  THEN 1 ELSE 0 END AS IS_HEALTHY_STOCK,

    -- Foreign keys
    dp.PRODUCT_HK                                    AS PRODUCT_SK,
    dd.DATE_KEY

FROM deduplicated d

LEFT JOIN {{ ref('dim_product') }} dp
    ON d.PRODUCT_ID = dp.PRODUCT_ID

LEFT JOIN {{ ref('dim_date') }} dd
    ON d.SNAPSHOT_DATE = dd.DATE_KEY

WHERE d.rn = 1
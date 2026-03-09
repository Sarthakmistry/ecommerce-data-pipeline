-- Grain: one row per order line item
SELECT
    o.ORDER_ID,
    o.ORDER_DATE,
    o.TOTAL_AMOUNT,
    o.STATUS,
    dc.CUSTOMER_SK,
    dp.PRODUCT_HK             AS PRODUCT_SK,
    dd.DATE_KEY,
    o.TOTAL_AMOUNT            AS REVENUE,
    CASE WHEN r.RETURN_ID IS NOT NULL THEN 1 ELSE 0 END AS IS_RETURNED
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('dim_customer') }} dc
    ON o.CUSTOMER_ID = dc.CUSTOMER_ID
    AND o.ORDER_DATE BETWEEN dc.DBT_VALID_FROM AND
        COALESCE(dc.DBT_VALID_TO, '9999-12-31'::DATE)
LEFT JOIN {{ ref('dim_product') }} dp
    ON o.PRODUCT_ID = dp.PRODUCT_ID
LEFT JOIN {{ ref('dim_date') }} dd
    ON o.ORDER_DATE = dd.DATE_KEY
LEFT JOIN {{ ref('stg_returns') }} r
    ON o.ORDER_ID = r.ORDER_ID

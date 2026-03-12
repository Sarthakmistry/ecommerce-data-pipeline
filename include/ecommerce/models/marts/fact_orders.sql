-- models/marts/fact_orders.sql
-- Direct dim_customer join used instead of customer_snapshot SCD2 join.
-- Snapshot join (ORDER_DATE BETWEEN DBT_VALID_FROM AND DBT_VALID_TO) fails
-- for backdated Faker orders since snapshot only has today's DBT_VALID_FROM.

SELECT
    o.ORDER_ID,
    o.ORDER_DATE,
    o.TOTAL_AMOUNT,
    o.STATUS,

    -- Surrogate keys
    dc.CUSTOMER_HK                                          AS CUSTOMER_SK,
    dp.PRODUCT_HK                                          AS PRODUCT_SK,
    dd.DATE_KEY,

    o.TOTAL_AMOUNT                                         AS REVENUE,
    CASE WHEN r.RETURN_ID IS NOT NULL THEN 1 ELSE 0 END    AS IS_RETURNED

FROM {{ ref('stg_orders') }} o

LEFT JOIN {{ ref('dim_customer') }} dc
    ON o.CUSTOMER_ID = dc.CUSTOMER_ID

LEFT JOIN {{ ref('dim_product') }} dp
    ON o.PRODUCT_ID = dp.PRODUCT_ID

LEFT JOIN {{ ref('dim_date') }} dd
    ON o.ORDER_DATE = dd.DATE_KEY

LEFT JOIN {{ ref('stg_returns') }} r
    ON o.ORDER_ID = r.ORDER_ID
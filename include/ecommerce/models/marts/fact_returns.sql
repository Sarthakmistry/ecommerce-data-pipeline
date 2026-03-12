-- models/marts/fact_returns.sql
-- Direct dim_customer join used instead of customer_snapshot SCD2 join.
-- Same reasoning as fact_orders — backdated return dates don't fall within
-- today's snapshot window, causing all CUSTOMER_SK to be NULL.

SELECT
    r.RETURN_ID,
    r.ORDER_ID,
    r.RETURN_DATE,
    r.RETURN_REASON,
    r.REFUND_AMOUNT,

    -- Surrogate keys
    dc.CUSTOMER_HK                                              AS CUSTOMER_SK,
    dp.PRODUCT_HK                                              AS PRODUCT_SK,
    dd.DATE_KEY,

    -- Return reason flags
    CASE WHEN r.RETURN_REASON = 'DAMAGED'          THEN 1 ELSE 0 END  AS IS_DAMAGED_RETURN,
    CASE WHEN r.RETURN_REASON = 'WRONG_SIZE'       THEN 1 ELSE 0 END  AS IS_WRONG_SIZE,
    CASE WHEN r.RETURN_REASON = 'NOT_AS_DESCRIBED' THEN 1 ELSE 0 END  AS IS_NOT_AS_DESCRIBED,
    CASE WHEN r.RETURN_REASON = 'CHANGED_MIND'     THEN 1 ELSE 0 END  AS IS_CHANGED_MIND

FROM {{ ref('stg_returns') }} r

LEFT JOIN {{ ref('stg_orders') }} o
    ON r.ORDER_ID = o.ORDER_ID

LEFT JOIN {{ ref('dim_customer') }} dc
    ON r.CUSTOMER_ID = dc.CUSTOMER_ID

LEFT JOIN {{ ref('dim_product') }} dp
    ON o.PRODUCT_ID = dp.PRODUCT_ID

LEFT JOIN {{ ref('dim_date') }} dd
    ON r.RETURN_DATE = dd.DATE_KEY
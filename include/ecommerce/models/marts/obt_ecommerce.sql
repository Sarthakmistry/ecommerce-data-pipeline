-- models/marts/obt_ecommerce.sql
-- One Big Table: Denormalized mart combining orders, customers, products,
-- returns, and inventory with rich Snowflake window function analytics.
-- Grain: one row per order.
--
-- Ingest DAG generates 400 backdated orders (past year) + 100 today's orders,
-- making all time-series aggregations meaningful.

{{ config(materialized='table') }}

WITH orders_base AS (
    SELECT
        o.ORDER_ID,
        o.ORDER_DATE,
        o.STATUS,
        o.TOTAL_AMOUNT,
        o.IS_RETURNED,

        -- Customer fields
        c.CUSTOMER_ID,
        c.FIRST_NAME,
        c.LAST_NAME,
        c.EMAIL,
        c.CITY,
        c.STATE,
        c.COUNTRY,

        -- Product fields
        p.PRODUCT_ID,
        o.PRODUCT_SK,
        p.PRODUCT_NAME,
        p.BRAND,
        p.CATEGORY,
        p.PRICE             AS UNIT_PRICE,
        p.RATING            AS PRODUCT_RATING,

        -- Date fields
        d.YEAR              AS ORDER_YEAR,
        d.MONTH             AS ORDER_MONTH,
        d.MONTH_NAME        AS ORDER_MONTH_NAME,
        d.QUARTER           AS ORDER_QUARTER,
        d.DAY_NAME          AS ORDER_DAY_NAME,
        d.IS_WEEKEND        AS IS_WEEKEND_ORDER

    FROM {{ ref('fact_orders') }} o
    LEFT JOIN {{ ref('dim_customer') }}  c  ON o.CUSTOMER_SK = c.CUSTOMER_HK
    LEFT JOIN {{ ref('dim_product') }}   p  ON o.PRODUCT_SK  = p.PRODUCT_HK
    LEFT JOIN {{ ref('dim_date') }}      d  ON o.DATE_KEY    = d.DATE_KEY
),

returns_info AS (
    SELECT
        r.ORDER_ID,
        r.RETURN_DATE,
        r.RETURN_REASON,
        r.REFUND_AMOUNT
    FROM {{ ref('fact_returns') }} r
),

inventory_latest AS (
    -- Most recent stock snapshot per product
    SELECT
        PRODUCT_SK,
        STOCK_LEVEL         AS LATEST_STOCK_LEVEL,
        WAREHOUSE_LOCATION  AS PRIMARY_WAREHOUSE,
        IS_OUT_OF_STOCK,
        IS_LOW_STOCK
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY PRODUCT_SK
                ORDER BY SNAPSHOT_DATE DESC
            ) AS rn
        FROM {{ ref('fact_inventory') }}
    )
    WHERE rn = 1
),

-- ============================================================
-- PRE-AGGREGATION CTEs
-- Snowflake does not allow nesting window functions inside other
-- window functions. All aggregates needed as ORDER BY / ranking
-- inputs are pre-computed here first.
-- ============================================================

-- Total revenue & order count per customer
customer_aggs AS (
    SELECT
        CUSTOMER_ID,
        SUM(TOTAL_AMOUNT)   AS CUSTOMER_TOTAL_REVENUE,
        COUNT(ORDER_ID)     AS CUSTOMER_ORDER_COUNT
    FROM orders_base
    GROUP BY CUSTOMER_ID
),

-- Total revenue per product
product_aggs AS (
    SELECT
        PRODUCT_ID,
        SUM(TOTAL_AMOUNT)   AS PRODUCT_TOTAL_REVENUE
    FROM orders_base
    GROUP BY PRODUCT_ID
),

-- Monthly revenue per category
category_month_aggs AS (
    SELECT
        CATEGORY,
        ORDER_YEAR,
        ORDER_MONTH,
        SUM(TOTAL_AMOUNT)   AS CATEGORY_MONTHLY_REVENUE
    FROM orders_base
    GROUP BY CATEGORY, ORDER_YEAR, ORDER_MONTH
),

-- LAG applied on the already-aggregated monthly totals
category_mom AS (
    SELECT
        CATEGORY,
        ORDER_YEAR,
        ORDER_MONTH,
        CATEGORY_MONTHLY_REVENUE,
        LAG(CATEGORY_MONTHLY_REVENUE) OVER (
            PARTITION BY CATEGORY
            ORDER BY ORDER_YEAR, ORDER_MONTH
        )                   AS CATEGORY_PREV_MONTH_REVENUE
    FROM category_month_aggs
),

-- ============================================================
-- WINDOW FUNCTION LAYER
-- ============================================================
enriched AS (
    SELECT
        ob.*,

        -- Return info
        ri.RETURN_DATE,
        ri.RETURN_REASON,
        ri.REFUND_AMOUNT,

        -- Inventory info
        il.LATEST_STOCK_LEVEL,
        il.PRIMARY_WAREHOUSE,
        il.IS_OUT_OF_STOCK,
        il.IS_LOW_STOCK,

        -- Pre-aggregated values (needed downstream in final SELECT)
        ca.CUSTOMER_TOTAL_REVENUE,
        ca.CUSTOMER_ORDER_COUNT,
        pa.PRODUCT_TOTAL_REVENUE,
        cm.CATEGORY_MONTHLY_REVENUE,
        cm.CATEGORY_PREV_MONTH_REVENUE,

        -- ── 1. CUSTOMER LIFETIME VALUE METRICS ──────────────────
        ca.CUSTOMER_TOTAL_REVENUE                           AS CUSTOMER_LIFETIME_VALUE,
        ca.CUSTOMER_ORDER_COUNT                             AS CUSTOMER_TOTAL_ORDERS,

        AVG(ob.TOTAL_AMOUNT) OVER (
            PARTITION BY ob.CUSTOMER_ID
        )                                                   AS CUSTOMER_AVG_ORDER_VALUE,

        -- ── 2. CUSTOMER ORDER SEQUENCING ─────────────────────────
        ROW_NUMBER() OVER (
            PARTITION BY ob.CUSTOMER_ID
            ORDER BY ob.ORDER_DATE, ob.ORDER_ID
        )                                                   AS CUSTOMER_ORDER_SEQUENCE,

        -- Days since customer's previous order
        DATEDIFF(
            'day',
            LAG(ob.ORDER_DATE) OVER (
                PARTITION BY ob.CUSTOMER_ID
                ORDER BY ob.ORDER_DATE, ob.ORDER_ID
            ),
            ob.ORDER_DATE
        )                                                   AS DAYS_SINCE_LAST_ORDER,

        -- Customer's next order date
        LEAD(ob.ORDER_DATE) OVER (
            PARTITION BY ob.CUSTOMER_ID
            ORDER BY ob.ORDER_DATE, ob.ORDER_ID
        )                                                   AS NEXT_ORDER_DATE,

        -- First and last order dates
        MIN(ob.ORDER_DATE) OVER (
            PARTITION BY ob.CUSTOMER_ID
        )                                                   AS CUSTOMER_FIRST_ORDER_DATE,

        MAX(ob.ORDER_DATE) OVER (
            PARTITION BY ob.CUSTOMER_ID
        )                                                   AS CUSTOMER_LAST_ORDER_DATE,

        -- ── 3. REVENUE RANKING ───────────────────────────────────
        DENSE_RANK() OVER (
            ORDER BY ca.CUSTOMER_TOTAL_REVENUE DESC
        )                                                   AS CUSTOMER_REVENUE_RANK,

        DENSE_RANK() OVER (
            ORDER BY pa.PRODUCT_TOTAL_REVENUE DESC
        )                                                   AS PRODUCT_REVENUE_RANK,

        -- ── 4. RUNNING & ROLLING TOTALS ──────────────────────────
        -- Cumulative revenue per customer over time
        SUM(ob.TOTAL_AMOUNT) OVER (
            PARTITION BY ob.CUSTOMER_ID
            ORDER BY ob.ORDER_DATE, ob.ORDER_ID
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                   AS CUSTOMER_RUNNING_REVENUE,

        -- 30-day rolling revenue per product
        SUM(ob.TOTAL_AMOUNT) OVER (
            PARTITION BY ob.PRODUCT_ID
            ORDER BY ob.ORDER_DATE
            RANGE BETWEEN INTERVAL '30 DAYS' PRECEDING AND CURRENT ROW
        )                                                   AS PRODUCT_30D_ROLLING_REVENUE,

        -- ── 5. PRODUCT PERFORMANCE WITHIN CATEGORY ───────────────
        RANK() OVER (
            PARTITION BY ob.CATEGORY
            ORDER BY pa.PRODUCT_TOTAL_REVENUE DESC
        )                                                   AS PRODUCT_RANK_IN_CATEGORY,

        ROUND(
            100.0 * pa.PRODUCT_TOTAL_REVENUE
            / NULLIF(
                SUM(pa.PRODUCT_TOTAL_REVENUE) OVER (PARTITION BY ob.CATEGORY),
                0
            ),
            2
        )                                                   AS PRODUCT_CATEGORY_REVENUE_SHARE_PCT,

        -- ── 6. RETURN METRICS ────────────────────────────────────
        -- Running return count per customer over time
        SUM(ob.IS_RETURNED) OVER (
            PARTITION BY ob.CUSTOMER_ID
            ORDER BY ob.ORDER_DATE, ob.ORDER_ID
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                   AS CUSTOMER_RUNNING_RETURN_COUNT,

        ROUND(
            100.0 * AVG(ob.IS_RETURNED) OVER (PARTITION BY ob.PRODUCT_ID),
            2
        )                                                   AS PRODUCT_RETURN_RATE_PCT,

        -- ── 7. CUSTOMER SEGMENTATION (RFM-STYLE) ─────────────────
        -- Recency: days between customer's last order and the dataset's max date
        DATEDIFF(
            'day',
            MAX(ob.ORDER_DATE) OVER (PARTITION BY ob.CUSTOMER_ID),
            MAX(ob.ORDER_DATE) OVER ()
        )                                                   AS CUSTOMER_RECENCY_DAYS,

        NTILE(4) OVER (
            ORDER BY ca.CUSTOMER_ORDER_COUNT
        )                                                   AS CUSTOMER_FREQUENCY_QUARTILE,

        NTILE(4) OVER (
            ORDER BY ca.CUSTOMER_TOTAL_REVENUE
        )                                                   AS CUSTOMER_MONETARY_QUARTILE

    FROM orders_base ob
    LEFT JOIN returns_info      ri ON ob.ORDER_ID    = ri.ORDER_ID
    LEFT JOIN inventory_latest  il ON ob.PRODUCT_SK  = il.PRODUCT_SK
    LEFT JOIN customer_aggs     ca ON ob.CUSTOMER_ID = ca.CUSTOMER_ID
    LEFT JOIN product_aggs      pa ON ob.PRODUCT_ID  = pa.PRODUCT_ID
    LEFT JOIN category_mom      cm ON ob.CATEGORY    = cm.CATEGORY
                                  AND ob.ORDER_YEAR  = cm.ORDER_YEAR
                                  AND ob.ORDER_MONTH = cm.ORDER_MONTH
)

-- ── FINAL SELECT + DERIVED LABELS ────────────────────────────
SELECT
    -- Keys & IDs
    ORDER_ID,
    CUSTOMER_ID,
    PRODUCT_ID,

    -- Order details
    ORDER_DATE,
    STATUS,
    TOTAL_AMOUNT,
    UNIT_PRICE,
    IS_RETURNED,
    RETURN_DATE,
    RETURN_REASON,
    REFUND_AMOUNT,

    -- Customer details
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    CITY,
    STATE,
    COUNTRY,

    -- Product details
    PRODUCT_NAME,
    BRAND,
    CATEGORY,
    PRODUCT_RATING,

    -- Date attributes
    ORDER_YEAR,
    ORDER_MONTH,
    ORDER_MONTH_NAME,
    ORDER_QUARTER,
    ORDER_DAY_NAME,
    IS_WEEKEND_ORDER,

    -- Inventory
    LATEST_STOCK_LEVEL,
    PRIMARY_WAREHOUSE,
    IS_OUT_OF_STOCK,
    IS_LOW_STOCK,

    -- Customer lifetime metrics
    CUSTOMER_LIFETIME_VALUE,
    CUSTOMER_TOTAL_ORDERS,
    CUSTOMER_AVG_ORDER_VALUE,
    CUSTOMER_ORDER_SEQUENCE,
    DAYS_SINCE_LAST_ORDER,
    NEXT_ORDER_DATE,
    CUSTOMER_FIRST_ORDER_DATE,
    CUSTOMER_LAST_ORDER_DATE,
    CUSTOMER_REVENUE_RANK,
    CUSTOMER_RUNNING_REVENUE,
    CUSTOMER_RUNNING_RETURN_COUNT,
    CUSTOMER_RECENCY_DAYS,
    CUSTOMER_FREQUENCY_QUARTILE,
    CUSTOMER_MONETARY_QUARTILE,

    -- Product performance metrics
    PRODUCT_REVENUE_RANK,
    PRODUCT_RANK_IN_CATEGORY,
    PRODUCT_CATEGORY_REVENUE_SHARE_PCT,
    PRODUCT_30D_ROLLING_REVENUE,
    PRODUCT_RETURN_RATE_PCT,

    -- Period-over-period metrics
    CATEGORY_MONTHLY_REVENUE,
    CATEGORY_PREV_MONTH_REVENUE,
    ROUND(
        100.0 * (CATEGORY_MONTHLY_REVENUE - CATEGORY_PREV_MONTH_REVENUE)
        / NULLIF(CATEGORY_PREV_MONTH_REVENUE, 0),
        2
    )                                               AS CATEGORY_MOM_REVENUE_GROWTH_PCT,

    -- ── DERIVED SEGMENT LABELS ────────────────────────────────
    -- RFM-style customer segment
    CASE
        WHEN CUSTOMER_MONETARY_QUARTILE = 4
         AND CUSTOMER_FREQUENCY_QUARTILE = 4  THEN 'Champions'
        WHEN CUSTOMER_MONETARY_QUARTILE >= 3
         AND CUSTOMER_FREQUENCY_QUARTILE >= 3  THEN 'Loyal Customers'
        WHEN CUSTOMER_RECENCY_DAYS <= 30       THEN 'Recent Customers'
        WHEN CUSTOMER_MONETARY_QUARTILE = 4    THEN 'Big Spenders'
        WHEN CUSTOMER_RECENCY_DAYS > 180       THEN 'At Risk'
        ELSE 'Occasional Buyers'
    END                                             AS CUSTOMER_SEGMENT,

    -- Order value tier
    CASE
        WHEN TOTAL_AMOUNT >= 400 THEN 'High Value'
        WHEN TOTAL_AMOUNT >= 150 THEN 'Mid Value'
        ELSE 'Low Value'
    END                                             AS ORDER_VALUE_TIER,

    -- Product performance tier within category
    CASE
        WHEN PRODUCT_RANK_IN_CATEGORY = 1   THEN 'Category Leader'
        WHEN PRODUCT_RANK_IN_CATEGORY <= 3  THEN 'Top 3'
        WHEN PRODUCT_RANK_IN_CATEGORY <= 10 THEN 'Top 10'
        ELSE 'Standard'
    END                                             AS PRODUCT_PERFORMANCE_TIER

FROM enriched
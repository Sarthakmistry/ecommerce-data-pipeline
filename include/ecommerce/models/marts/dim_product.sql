-- Reads from Vault, joins Hub + latest Satellite
SELECT
    h.PRODUCT_HK,
    h.PRODUCT_BK              AS PRODUCT_ID,
    s.PRODUCT_NAME,
    s.BRAND,
    s.CATEGORY,
    s.PRICE,
    s.STOCK_QUANTITY,
    s.RATING,
    s.LOAD_DATE               AS EFFECTIVE_FROM
FROM {{ ref('hub_product') }} h
JOIN {{ ref('sat_product_details') }} s
    ON h.PRODUCT_HK = s.PRODUCT_HK
    AND s.END_DATE IS NULL    -- current record only
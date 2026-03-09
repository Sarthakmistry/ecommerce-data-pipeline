SELECT
    h.CUSTOMER_HK,
    h.CUSTOMER_HK       AS CUSTOMER_SK,   -- alias for fact table joins
    h.CUSTOMER_BK       AS customer_id,
    s.FIRST_NAME,
    s.LAST_NAME,
    s.EMAIL,
    s.PHONE,
    s.SHIPPING_ADDRESS,
    s.CITY,
    s.STATE,
    s.COUNTRY,
    s.LOAD_DATE         AS effective_from
FROM {{ ref('hub_customer') }} h
JOIN {{ ref('sat_customer_details') }} s
    ON h.CUSTOMER_HK = s.CUSTOMER_HK
    AND s.END_DATE IS NULL
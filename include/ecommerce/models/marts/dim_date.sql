-- Generates a date spine from 2020-01-01 to 2030-12-31
WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), '2020-01-01'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 4000))
)
SELECT
    date_day                            AS date_key,
    YEAR(date_day)                      AS year,
    MONTH(date_day)                     AS month,
    DAY(date_day)                       AS day,
    DAYOFWEEK(date_day)                 AS day_of_week,
    DAYNAME(date_day)                   AS day_name,
    MONTHNAME(date_day)                 AS month_name,
    QUARTER(date_day)                   AS quarter,
    WEEKOFYEAR(date_day)                AS week_of_year,
    CASE WHEN DAYOFWEEK(date_day) IN (1,7)
         THEN TRUE ELSE FALSE END       AS is_weekend
FROM date_spine

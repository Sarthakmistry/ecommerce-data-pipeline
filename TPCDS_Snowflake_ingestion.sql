USE ROLE PIPELINE_ROLE;
USE WAREHOUSE ECOMM_WH;

-- Loading TPC-DS 10GB dataset from public S3 bucket into Snowflake. The dataset is in pipe-delimited CSV format with empty fields representing NULL values.
CREATE OR REPLACE STAGE tpcds_public_stage
  URL = 's3://redshift-downloads/TPC-DS/2.13/10GB/'
  FILE_FORMAT = (
    TYPE = CSV 
    FIELD_DELIMITER = '|' 
    NULL_IF = ('') 
    EMPTY_FIELD_AS_NULL = TRUE
  );

-- Sample query to verify the data in the stage before loading into Snowflake tables
SELECT $1, $2, $3, $4, $5
FROM @tpcds_public_stage/store_sales/
LIMIT 5;

-- Store Sales
CREATE OR REPLACE TABLE RAW.TPCDS_STORE_SALES (
    ss_sold_date_sk       INTEGER,
    ss_sold_time_sk       INTEGER,
    ss_item_sk            INTEGER,
    ss_customer_sk        INTEGER,
    ss_cdemo_sk           INTEGER,
    ss_hdemo_sk           INTEGER,
    ss_addr_sk            INTEGER,
    ss_store_sk           INTEGER,
    ss_promo_sk           INTEGER,
    ss_ticket_number      BIGINT,
    ss_quantity           INTEGER,
    ss_wholesale_cost     NUMBER(7,2),
    ss_list_price         NUMBER(7,2),
    ss_sales_price        NUMBER(7,2),
    ss_ext_discount_amt   NUMBER(7,2),
    ss_ext_sales_price    NUMBER(7,2),
    ss_ext_wholesale_cost NUMBER(7,2),
    ss_ext_list_price     NUMBER(7,2),
    ss_ext_tax            NUMBER(7,2),
    ss_coupon_amt         NUMBER(7,2),
    ss_net_paid           NUMBER(7,2),
    ss_net_paid_inc_tax   NUMBER(7,2),
    ss_net_profit         NUMBER(7,2)
);

COPY INTO RAW.TPCDS_STORE_SALES
FROM @tpcds_public_stage/store_sales/
FILE_FORMAT = (
    TYPE                = CSV
    FIELD_DELIMITER     = '|'
    NULL_IF             = ('')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = CONTINUE;

-- Customer
CREATE OR REPLACE TABLE RAW.TPCDS_CUSTOMER (
    c_customer_sk         INTEGER,
    c_customer_id         VARCHAR(16),
    c_current_cdemo_sk    INTEGER,
    c_current_hdemo_sk    INTEGER,
    c_current_addr_sk     INTEGER,
    c_first_shipto_date_sk INTEGER,
    c_first_sales_date_sk INTEGER,
    c_salutation          VARCHAR(10),
    c_first_name          VARCHAR(20),
    c_last_name           VARCHAR(30),
    c_preferred_cust_flag VARCHAR(1),
    c_birth_day           INTEGER,
    c_birth_month         INTEGER,
    c_birth_year          INTEGER,
    c_birth_country       VARCHAR(20),
    c_login               VARCHAR(13),
    c_email_address       VARCHAR(50),
    c_last_review_date_sk INTEGER
);

COPY INTO RAW.TPCDS_CUSTOMER
FROM @tpcds_public_stage/customer/
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER='|' NULL_IF=('') EMPTY_FIELD_AS_NULL=TRUE ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE)
ON_ERROR = CONTINUE;

-- ITEM
CREATE OR REPLACE TABLE RAW.TPCDS_ITEM (
    i_item_sk             INTEGER,
    i_item_id             VARCHAR(16),
    i_rec_start_date      DATE,
    i_rec_end_date        DATE,
    i_item_desc           VARCHAR(200),
    i_current_price       NUMBER(7,2),
    i_wholesale_cost      NUMBER(7,2),
    i_brand_id            INTEGER,
    i_brand               VARCHAR(50),
    i_class_id            INTEGER,
    i_class               VARCHAR(50),
    i_category_id         INTEGER,
    i_category            VARCHAR(50),
    i_manufact_id         INTEGER,
    i_manufact            VARCHAR(50),
    i_size                VARCHAR(20),
    i_formulation         VARCHAR(20),
    i_color               VARCHAR(20),
    i_units               VARCHAR(10),
    i_container           VARCHAR(10),
    i_manager_id          INTEGER,
    i_product_name        VARCHAR(50)
);

COPY INTO RAW.TPCDS_ITEM
FROM @tpcds_public_stage/item/
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER='|' NULL_IF=('') EMPTY_FIELD_AS_NULL=TRUE ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE)
ON_ERROR = CONTINUE;


-- STORE_RETURNS
CREATE OR REPLACE TABLE RAW.TPCDS_STORE_RETURNS (
    sr_returned_date_sk   INTEGER,
    sr_return_time_sk     INTEGER,
    sr_item_sk            INTEGER,
    sr_customer_sk        INTEGER,
    sr_cdemo_sk           INTEGER,
    sr_hdemo_sk           INTEGER,
    sr_addr_sk            INTEGER,
    sr_store_sk           INTEGER,
    sr_reason_sk          INTEGER,
    sr_ticket_number      BIGINT,
    sr_return_quantity    INTEGER,
    sr_return_amt         NUMBER(7,2),
    sr_return_tax         NUMBER(7,2),
    sr_return_amt_inc_tax NUMBER(7,2),
    sr_fee                NUMBER(7,2),
    sr_return_ship_cost   NUMBER(7,2),
    sr_refunded_cash      NUMBER(7,2),
    sr_reversed_charge    NUMBER(7,2),
    sr_store_credit       NUMBER(7,2),
    sr_net_loss           NUMBER(7,2)
);

COPY INTO RAW.TPCDS_STORE_RETURNS
FROM @tpcds_public_stage/store_returns/
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER='|' NULL_IF=('') EMPTY_FIELD_AS_NULL=TRUE ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE)
ON_ERROR = CONTINUE;

-- DATE_DIM
CREATE OR REPLACE TABLE RAW.TPCDS_DATE_DIM (
    d_date_sk             INTEGER,
    d_date_id             VARCHAR(16),
    d_date                DATE,
    d_month_seq           INTEGER,
    d_week_seq            INTEGER,
    d_quarter_seq         INTEGER,
    d_year                INTEGER,
    d_dow                 INTEGER,
    d_moy                 INTEGER,
    d_dom                 INTEGER,
    d_qoy                 INTEGER,
    d_fy_year             INTEGER,
    d_fy_quarter_seq      INTEGER,
    d_fy_week_seq         INTEGER,
    d_day_name            VARCHAR(9),
    d_quarter_name        VARCHAR(6),
    d_holiday             VARCHAR(1),
    d_weekend             VARCHAR(1),
    d_following_holiday   VARCHAR(1),
    d_first_dom           INTEGER,
    d_last_dom            INTEGER,
    d_same_day_ly         INTEGER,
    d_same_day_lq         INTEGER,
    d_current_day         VARCHAR(1),
    d_current_week        VARCHAR(1),
    d_current_month       VARCHAR(1),
    d_current_quarter     VARCHAR(1),
    d_current_year        VARCHAR(1)
);

COPY INTO RAW.TPCDS_DATE_DIM
FROM @tpcds_public_stage/date_dim/
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER='|' NULL_IF=('') EMPTY_FIELD_AS_NULL=TRUE ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE)
ON_ERROR = CONTINUE;

--sanity check
SELECT 'TPCDS_STORE_SALES'  AS tbl, COUNT(*) AS row_count FROM RAW.TPCDS_STORE_SALES  UNION ALL
SELECT 'TPCDS_CUSTOMER',              COUNT(*)             FROM RAW.TPCDS_CUSTOMER      UNION ALL
SELECT 'TPCDS_ITEM',                  COUNT(*)             FROM RAW.TPCDS_ITEM           UNION ALL
SELECT 'TPCDS_STORE_RETURNS',         COUNT(*)             FROM RAW.TPCDS_STORE_RETURNS  UNION ALL
SELECT 'TPCDS_DATE_DIM',              COUNT(*)             FROM RAW.TPCDS_DATE_DIM
ORDER BY tbl;
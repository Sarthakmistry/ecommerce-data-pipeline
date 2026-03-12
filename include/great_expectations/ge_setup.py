import great_expectations as gx
from great_expectations.core import ExpectationSuite
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from datetime import datetime

load_dotenv(os.path.join(os.path.dirname(__file__), '../../.env'))

account   = os.getenv('SNOWFLAKE_ACCOUNT')
user      = os.getenv('SNOWFLAKE_USER')
password  = quote_plus(os.getenv('SNOWFLAKE_PASSWORD'))
database  = os.getenv('SNOWFLAKE_DATABASE')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
role      = os.getenv('SNOWFLAKE_ROLE')

context = gx.get_context(
    mode='file',
    project_root_dir=os.path.join(os.path.dirname(__file__), '.')
)

today = datetime.today().strftime('%Y-%m-%d')


# DATASOURCES

staging_ds = context.data_sources.add_or_update_snowflake(
    name='snowflake_staging',
    connection_string=(
        f'snowflake://{user}:{password}@{account}/'
        f'{database}/STAGING'
        f'?warehouse={warehouse}&role={role}'
    )
)

marts_ds = context.data_sources.add_or_update_snowflake(
    name='snowflake_marts',
    connection_string=(
        f'snowflake://{user}:{password}@{account}/'
        f'{database}/MARTS'
        f'?warehouse={warehouse}&role={role}'
    )
)


# TABLE ASSETS — STAGING

products_asset  = staging_ds.add_table_asset(name='stg_products',  table_name='STG_PRODUCTS')
customers_asset = staging_ds.add_table_asset(name='stg_customers', table_name='STG_CUSTOMERS')
orders_asset    = staging_ds.add_table_asset(name='stg_orders',    table_name='STG_ORDERS')
returns_asset   = staging_ds.add_table_asset(name='stg_returns',   table_name='STG_RETURNS')

products_batch  = products_asset.add_batch_definition_whole_table('full_table')
customers_batch = customers_asset.add_batch_definition_whole_table('full_table')
orders_batch    = orders_asset.add_batch_definition_whole_table('full_table')
returns_batch   = returns_asset.add_batch_definition_whole_table('full_table')


# TABLE ASSETS — MARTS

fact_orders_asset   = marts_ds.add_table_asset(name='fact_orders',   table_name='FACT_ORDERS')
fact_returns_asset  = marts_ds.add_table_asset(name='fact_returns',  table_name='FACT_RETURNS')
obt_asset           = marts_ds.add_table_asset(name='obt_ecommerce', table_name='OBT_ECOMMERCE')

fact_orders_batch   = fact_orders_asset.add_batch_definition_whole_table('full_table')
fact_returns_batch  = fact_returns_asset.add_batch_definition_whole_table('full_table')
obt_batch           = obt_asset.add_batch_definition_whole_table('full_table')


# SUITES — STAGING (existing + enhanced)

# ── stg_products
products_suite = context.suites.add_or_update(ExpectationSuite(name='stg_products_suite'))
products_suite.add_expectation(gx.expectations.ExpectColumnToExist(column='PRODUCT_ID'))
products_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='PRODUCT_ID'))
products_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column='PRODUCT_ID'))
products_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='PRODUCT_NAME'))
products_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='PRICE', min_value=0, max_value=100000
))
products_suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(
    min_value=1, max_value=100000
))
# price should be realistic — warn if average is suspiciously low or high
products_suite.add_expectation(gx.expectations.ExpectColumnMeanToBeBetween(
    column='PRICE', min_value=5, max_value=2000
))
# rating must be within 0–5 star scale
products_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='RATING', min_value=0, max_value=5
))
# stock quantity should be non-negative
products_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='STOCK_QUANTITY', min_value=0, max_value=100000
))

#  stg_orders 
orders_suite = context.suites.add_or_update(ExpectationSuite(name='stg_orders_suite'))
orders_suite.add_expectation(gx.expectations.ExpectColumnToExist(column='ORDER_ID'))
orders_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='ORDER_ID'))
orders_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column='ORDER_ID'))
orders_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column='STATUS',
    value_set=['PENDING', 'SHIPPED', 'DELIVERED', 'CANCELLED']
))
# order date must be within realistic range (not future, not ancient)
orders_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='ORDER_DATE', min_value='2020-01-01', max_value=today
))
# today's data must be present — freshness check
orders_suite.add_expectation(gx.expectations.ExpectColumnMaxToBeBetween(
    column='ORDER_DATE', min_value=today
))
# total amount must be positive and reasonable
orders_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='TOTAL_AMOUNT', min_value=0.01, max_value=100000
))
# statistical — revenue mean should be in expected range for Faker data
orders_suite.add_expectation(gx.expectations.ExpectColumnMeanToBeBetween(
    column='TOTAL_AMOUNT', min_value=50, max_value=400
))
# volume check — 500 orders per daily run expected
orders_suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(
    min_value=100, max_value=100000
))

# stg_customers 
customers_suite = context.suites.add_or_update(ExpectationSuite(name='stg_customers_suite'))
customers_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='CUSTOMER_ID'))
customers_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column='CUSTOMER_ID'))
customers_suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
    column='EMAIL', regex=r'^[^@]+@[^@]+\.[^@]+$'
))
# exactly 100 customers expected from DummyJSON
customers_suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(
    min_value=100, max_value=100
))
# email uniqueness across the table
customers_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column='EMAIL'))

#  stg_returns 
returns_suite = context.suites.add_or_update(ExpectationSuite(name='stg_returns_suite'))
returns_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='RETURN_ID'))
returns_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column='RETURN_REASON',
    value_set=['WRONG_SIZE', 'DAMAGED', 'NOT_AS_DESCRIBED', 'CHANGED_MIND']
))
# NEW: return date must not be in the future
returns_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='RETURN_DATE', min_value='2020-01-01', max_value=today
))
# NEW: refund must be positive
returns_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='REFUND_AMOUNT', min_value=0.01, max_value=100000
))
# NEW: today's returns must be present — freshness check
returns_suite.add_expectation(gx.expectations.ExpectColumnMaxToBeBetween(
    column='RETURN_DATE', min_value=today
))



# SUITES — MARTS 

# fact_orders 
fact_orders_suite = context.suites.add_or_update(ExpectationSuite(name='fact_orders_suite'))

# Structure
fact_orders_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='ORDER_ID'))
fact_orders_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column='ORDER_ID'))
fact_orders_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='CUSTOMER_SK'))
fact_orders_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='PRODUCT_SK'))

fact_orders_suite.add_expectation(gx.expectations.ExpectColumnDistinctValuesToContainSet(
    column='STATUS',
    value_set=['PENDING', 'SHIPPED', 'DELIVERED', 'CANCELLED']
))

# Revenue statistics
fact_orders_suite.add_expectation(gx.expectations.ExpectColumnMeanToBeBetween(
    column='TOTAL_AMOUNT', min_value=50, max_value=400
))
fact_orders_suite.add_expectation(gx.expectations.ExpectColumnStdevToBeBetween(
    column='TOTAL_AMOUNT', min_value=10, max_value=300
))
fact_orders_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='TOTAL_AMOUNT', min_value=0.01, max_value=100000
))

# Freshness — today's orders must be loaded
fact_orders_suite.add_expectation(gx.expectations.ExpectColumnMaxToBeBetween(
    column='ORDER_DATE', min_value=today
))

# Historical spread — data should go back at least 6 months
fact_orders_suite.add_expectation(gx.expectations.ExpectColumnMinToBeBetween(
    column='ORDER_DATE',
    min_value='2020-01-01',
    max_value=(datetime.today().replace(month=max(1, datetime.today().month - 6))).strftime('%Y-%m-%d')
))

# IS_RETURNED must be 0 or 1
fact_orders_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='IS_RETURNED', min_value=0, max_value=1
))

# Volume
fact_orders_suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(
    min_value=100, max_value=10000000
))

# fact_returns 
fact_returns_suite = context.suites.add_or_update(ExpectationSuite(name='fact_returns_suite'))

fact_returns_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='RETURN_ID'))
fact_returns_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column='RETURN_ID'))
fact_returns_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='CUSTOMER_SK'))
fact_returns_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column='RETURN_REASON',
    value_set=['WRONG_SIZE', 'DAMAGED', 'NOT_AS_DESCRIBED', 'CHANGED_MIND']
))

# Refund amount must be positive
fact_returns_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='REFUND_AMOUNT', min_value=0.01, max_value=100000
))

# Cross-column: refund should not exceed a reasonable order ceiling
# (GX doesn't support direct cross-column comparison, so we cap at max order value)
fact_returns_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='REFUND_AMOUNT', min_value=0.01, max_value=500
))

# Freshness
fact_returns_suite.add_expectation(gx.expectations.ExpectColumnMaxToBeBetween(
    column='RETURN_DATE', min_value=today
))

# All 4 return reasons should appear across the dataset
fact_returns_suite.add_expectation(gx.expectations.ExpectColumnDistinctValuesToContainSet(
    column='RETURN_REASON',
    value_set=['WRONG_SIZE', 'DAMAGED', 'NOT_AS_DESCRIBED', 'CHANGED_MIND']
))

#  obt_ecommerce 
obt_suite = context.suites.add_or_update(ExpectationSuite(name='obt_ecommerce_suite'))

# Grain check
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='ORDER_ID'))
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column='ORDER_ID'))

# All join keys populated — if these are null, upstream joins failed
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='CUSTOMER_ID'))
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='PRODUCT_ID'))
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='FIRST_NAME'))
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='PRODUCT_NAME'))

# Window function outputs not null — if null, pre-agg CTEs failed
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='CUSTOMER_LIFETIME_VALUE'))
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='CUSTOMER_TOTAL_ORDERS'))
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='PRODUCT_REVENUE_RANK'))
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='CUSTOMER_SEGMENT'))

# Segment values
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column='CUSTOMER_SEGMENT',
    value_set=['Champions', 'Loyal Customers', 'Recent Customers', 'Big Spenders', 'At Risk', 'Occasional Buyers']
))

# Order value tier
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column='ORDER_VALUE_TIER',
    value_set=['High Value', 'Mid Value', 'Low Value']
))

# Product performance tier
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column='PRODUCT_PERFORMANCE_TIER',
    value_set=['Category Leader', 'Top 3', 'Top 10', 'Standard']
))

# RFM quartiles — must be 1–4
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='CUSTOMER_FREQUENCY_QUARTILE', min_value=1, max_value=4
))
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='CUSTOMER_MONETARY_QUARTILE', min_value=1, max_value=4
))

# Revenue metrics must be positive
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='CUSTOMER_LIFETIME_VALUE', min_value=0.01, max_value=10000000
))
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='PRODUCT_CATEGORY_REVENUE_SHARE_PCT', min_value=0, max_value=100
))
obt_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column='PRODUCT_RETURN_RATE_PCT', min_value=0, max_value=100
))

# Freshness — today's orders must exist in OBT
obt_suite.add_expectation(gx.expectations.ExpectColumnMaxToBeBetween(
    column='ORDER_DATE', min_value=today
))

# Segment diversity — all segments should appear (no single segment dominating completely)
obt_suite.add_expectation(gx.expectations.ExpectColumnDistinctValuesToContainSet(
    column='ORDER_VALUE_TIER',
    value_set=['High Value', 'Mid Value', 'Low Value']
))


# VALIDATION DEFINITIONS

# Staging
products_validation  = context.validation_definitions.add_or_update(gx.ValidationDefinition(
    name='stg_products_validation',  data=products_batch,  suite=products_suite))
customers_validation = context.validation_definitions.add_or_update(gx.ValidationDefinition(
    name='stg_customers_validation', data=customers_batch, suite=customers_suite))
orders_validation    = context.validation_definitions.add_or_update(gx.ValidationDefinition(
    name='stg_orders_validation',    data=orders_batch,    suite=orders_suite))
returns_validation   = context.validation_definitions.add_or_update(gx.ValidationDefinition(
    name='stg_returns_validation',   data=returns_batch,   suite=returns_suite))

# Marts
fact_orders_validation  = context.validation_definitions.add_or_update(gx.ValidationDefinition(
    name='fact_orders_validation',  data=fact_orders_batch,  suite=fact_orders_suite))
fact_returns_validation = context.validation_definitions.add_or_update(gx.ValidationDefinition(
    name='fact_returns_validation', data=fact_returns_batch, suite=fact_returns_suite))
obt_validation          = context.validation_definitions.add_or_update(gx.ValidationDefinition(
    name='obt_ecommerce_validation', data=obt_batch,         suite=obt_suite))


# CHECKPOINTS

# Staging checkpoint
context.checkpoints.add_or_update(
    gx.Checkpoint(
        name='staging_checkpoint',
        validation_definitions=[
            products_validation,
            customers_validation,
            orders_validation,
            returns_validation,
        ],
    )
)

# Marts checkpoint 
context.checkpoints.add_or_update(
    gx.Checkpoint(
        name='marts_checkpoint',
        validation_definitions=[
            fact_orders_validation,
            fact_returns_validation,
            obt_validation,
        ],
    )
)
# Building data docs
context.build_data_docs()
print("Data Docs built successfully!")

# Open local site in browser to verify
import webbrowser, os
local_docs = os.path.join(
    os.path.dirname(__file__),
    'uncommitted/data_docs/local_site/index.html'
)
webbrowser.open(f'file://{os.path.abspath(local_docs)}')

print('GE setup complete! Version:', gx.__version__)
print('Checkpoints registered: staging_checkpoint, marts_checkpoint')
import great_expectations as gx
from great_expectations.core import ExpectationSuite
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '../../.env'))

account   = os.getenv('SNOWFLAKE_ACCOUNT')
user      = os.getenv('SNOWFLAKE_USER')
password  = quote_plus(os.getenv('SNOWFLAKE_PASSWORD'))
database  = os.getenv('SNOWFLAKE_DATABASE')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
role      = os.getenv('SNOWFLAKE_ROLE')

connection_string = (
    f"snowflake://{user}:{password}@{account}/"
    f"{database}/STAGING"
    f"?warehouse={warehouse}&role={role}"
)

context = gx.get_context(
    mode="file",
    project_root_dir=os.path.join(os.path.dirname(__file__), '.')
)

# ── Datasource ──────────────────────────────────────────────
datasource = context.data_sources.add_or_update_snowflake(
    name="snowflake_staging",
    connection_string=connection_string
)

# ── Table Assets — one per staging table ────────────────────
products_asset = datasource.add_table_asset(
    name="stg_products",
    table_name="STG_PRODUCTS",
    schema_name="STAGING"
)

customers_asset = datasource.add_table_asset(
    name="stg_customers",
    table_name="STG_CUSTOMERS",
    schema_name="STAGING"
)

orders_asset = datasource.add_table_asset(
    name="stg_orders",
    table_name="STG_ORDERS",
    schema_name="STAGING"
)

returns_asset = datasource.add_table_asset(
    name="stg_returns",
    table_name="STG_RETURNS",
    schema_name="STAGING"
)

# ── Batch Definitions ────────────────────────────────────────
products_batch  = products_asset.add_batch_definition_whole_table("full_table")
customers_batch = customers_asset.add_batch_definition_whole_table("full_table")
orders_batch    = orders_asset.add_batch_definition_whole_table("full_table")
returns_batch   = returns_asset.add_batch_definition_whole_table("full_table")

# ── Expectation Suites ───────────────────────────────────────

# Products suite
products_suite = context.suites.add_or_update(
    ExpectationSuite(name="stg_products_suite")
)
products_suite.add_expectation(
    gx.expectations.ExpectColumnToExist(column="PRODUCT_ID"))
products_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="PRODUCT_ID"))
products_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="PRODUCT_ID"))
products_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="PRODUCT_NAME"))
products_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="PRICE", min_value=0, max_value=100000))
products_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="STOCK_QUANTITY", min_value=0, max_value=100000))
products_suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=1, max_value=100000))

# Customers suite
customers_suite = context.suites.add_or_update(
    ExpectationSuite(name="stg_customers_suite")
)
customers_suite.add_expectation(
    gx.expectations.ExpectColumnToExist(column="CUSTOMER_ID"))
customers_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="CUSTOMER_ID"))
customers_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="CUSTOMER_ID"))
customers_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="EMAIL"))
customers_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchRegex(
        column="EMAIL", regex=r"^[^@]+@[^@]+\.[^@]+$"))

# Orders suite
orders_suite = context.suites.add_or_update(
    ExpectationSuite(name="stg_orders_suite")
)
orders_suite.add_expectation(
    gx.expectations.ExpectColumnToExist(column="ORDER_ID"))
orders_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="ORDER_ID"))
orders_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="ORDER_ID"))
orders_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="CUSTOMER_ID"))
orders_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="STATUS",
        value_set=["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"]))
orders_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="TOTAL_AMOUNT", min_value=0, max_value=1000000))

# Returns suite
returns_suite = context.suites.add_or_update(
    ExpectationSuite(name="stg_returns_suite")
)
returns_suite.add_expectation(
    gx.expectations.ExpectColumnToExist(column="RETURN_ID"))
returns_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="RETURN_ID"))
returns_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="ORDER_ID"))
returns_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="RETURN_REASON",
        value_set=["WRONG_SIZE", "DAMAGED",
                   "NOT_AS_DESCRIBED", "CHANGED_MIND"]))

# ── Validation Definitions ───────────────────────────────────
products_validation = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="stg_products_validation",
        data=products_batch,
        suite=products_suite,
    )
)
customers_validation = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="stg_customers_validation",
        data=customers_batch,
        suite=customers_suite,
    )
)
orders_validation = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="stg_orders_validation",
        data=orders_batch,
        suite=orders_suite,
    )
)
returns_validation = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="stg_returns_validation",
        data=returns_batch,
        suite=returns_suite,
    )
)

# ── Checkpoint — all staging tables in one checkpoint ────────
checkpoint = context.checkpoints.add_or_update(
    gx.Checkpoint(
        name="staging_checkpoint",
        validation_definitions=[
            products_validation,
            customers_validation,
            orders_validation,
            returns_validation,
        ],
    )
)

# ── Build Data Docs ──────────────────────────────────────────
context.build_data_docs()
print("Data Docs built successfully!")

# Open local site in browser to verify
import webbrowser, os
local_docs = os.path.join(
    os.path.dirname(__file__),
    'uncommitted/data_docs/local_site/index.html'
)
webbrowser.open(f'file://{os.path.abspath(local_docs)}')

print("Great Expectations setup complete!")
print(f"GE version: {gx.__version__}")
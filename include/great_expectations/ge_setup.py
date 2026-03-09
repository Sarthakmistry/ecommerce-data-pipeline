import great_expectations as gx
import os
from dotenv import load_dotenv

# Load variables from .env into os.environ

load_dotenv(os.path.join(os.path.dirname(__file__), '../../.env'))

# Pull each value from environment
account    = os.getenv('SNOWFLAKE_ACCOUNT')
user       = os.getenv('SNOWFLAKE_USER')
password   = os.getenv('SNOWFLAKE_PASSWORD')
database   = os.getenv('SNOWFLAKE_DATABASE')
warehouse  = os.getenv('SNOWFLAKE_WAREHOUSE')
role       = os.getenv('SNOWFLAKE_ROLE')

# Build connection string from env vars
connection_string = (
    f"snowflake://{user}:{password}@{account}/"
    f"{database}/RAW"
    f"?warehouse={warehouse}&role={role}"
)

context = gx.get_context(
    mode="file",
    project_root_dir= os.path.join(os.path.dirname(__file__), '..')
)

datasource = context.sources.add_snowflake(
    name="snowflake_source",
    connection_string=connection_string
)

asset = datasource.add_table_asset(
    name="raw_products",
    table_name="RAW_PRODUCTS"
)

batch = asset.add_batch_definition_whole_table("full_table")

suite = context.add_expectation_suite("raw_products_suite")

suite.add_expectation(
    gx.expectations.ExpectColumnToExist(column="PRODUCT_ID"))
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="PRODUCT_ID"))
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="PRODUCT_ID"))
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=1, max_value=10000))

context.save_expectation_suite(suite)

checkpoint = context.add_checkpoint(
    name="raw_products_checkpoint",
    validations=[{
        "batch_request": batch.build_batch_request(),
        "expectation_suite_name": "raw_products_suite"
    }]
)

print("Great Expectations setup complete!")
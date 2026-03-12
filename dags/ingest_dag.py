from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests, json, uuid
from faker import Faker
import random
import pendulum
from datetime import datetime
from datetime import timedelta

fake = Faker()

default_args = {'owner': 'data_eng', 'retries': 2,
                'retry_delay': timedelta(minutes=5)}

@dag(dag_id='ecomm_ingest',
     schedule_interval='0 22 * * *',
     start_date=pendulum.datetime(2024, 1, 1, tz='America/New_York'),
     default_args=default_args,
     catchup=False, tags=['ingestion', 'ecommerce'])
def ingest_dag():

    @task()
    def ingest_products():
        resp = requests.get('https://dummyjson.com/products?limit=100')
        products = resp.json()['products']
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run("""
            CREATE TABLE IF NOT EXISTS RAW.RAW_PRODUCTS (
                PRODUCT_ID INT, RAW_DATA VARIANT,
                INGESTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )""")
        for p in products:
            hook.run(
                'INSERT INTO RAW.RAW_PRODUCTS(PRODUCT_ID, RAW_DATA) '
                'SELECT %s, PARSE_JSON(%s)',
                parameters=(p['id'], json.dumps(p)))

    @task()
    def ingest_users():
        resp = requests.get('https://dummyjson.com/users?limit=100')
        users = resp.json()['users']
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run("""
            CREATE TABLE IF NOT EXISTS RAW.RAW_CUSTOMERS (
                CUSTOMER_ID INT, RAW_DATA VARIANT,
                INGESTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )""")
        for u in users:
            hook.run(
                'INSERT INTO RAW.RAW_CUSTOMERS(CUSTOMER_ID, RAW_DATA) '
                'SELECT %s, PARSE_JSON(%s)',
                parameters=(u['id'], json.dumps(u)))

    @task()
    def generate_orders():
        historical = [
            {
                'order_id': str(uuid.uuid4()),
                'customer_id': random.randint(1, 100),
                'product_id': random.randint(1, 100),
                'status': random.choice(['PENDING','SHIPPED','DELIVERED','CANCELLED']),
                'shipping_address': fake.address(),
                'order_date': fake.date_between(start_date='-1y', end_date='-1d').isoformat(),
                'total_amount': round(random.uniform(20, 500), 2)
            }
            for _ in range(400)
        ]

        # 100 guaranteed today's orders — simulates daily operational load
        todays = [
            {
                'order_id': str(uuid.uuid4()),
                'customer_id': random.randint(1, 100),
                'product_id': random.randint(1, 100),
                'status': random.choice(['PENDING','SHIPPED','DELIVERED','CANCELLED']),
                'shipping_address': fake.address(),
                'order_date': datetime.today().date().isoformat(),
                'total_amount': round(random.uniform(20, 500), 2)
            }
            for _ in range(100)
        ]

        orders = historical + todays
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run("""
            CREATE TABLE IF NOT EXISTS RAW.RAW_ORDERS (
                ORDER_ID VARCHAR, RAW_DATA VARIANT,
                INGESTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )""")
        for o in orders:
            hook.run(
                'INSERT INTO RAW.RAW_ORDERS(ORDER_ID, RAW_DATA) '
                'SELECT %s, PARSE_JSON(%s)',
                parameters=(o['order_id'], json.dumps(o)))

    @task()
    def generate_returns():
        # Returns trigger SCD changes on order status
        reasons = ['WRONG_SIZE','DAMAGED','NOT_AS_DESCRIBED','CHANGED_MIND']
        historical_returns = [{
            'return_id': str(uuid.uuid4()),
            'order_id': str(uuid.uuid4()),
            'customer_id': random.randint(1,100),
            'reason': random.choice(reasons),
            'return_date': fake.date_between(start_date='-6m', end_date='-1d').isoformat(),
            'refund_amount': round(random.uniform(10,300),2)}
            for _ in range(60)
        ]
        todays_returns = [{
            'return_id': str(uuid.uuid4()),
            'order_id': str(uuid.uuid4()),
            'customer_id': random.randint(1,100),
            'reason': random.choice(reasons),
            'return_date': datetime.today().date().isoformat(),
            'refund_amount': round(random.uniform(10,300),2)}
            for _ in range(20)
        ]

        returns = historical_returns + todays_returns
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run("""
            CREATE TABLE IF NOT EXISTS RAW.RAW_RETURNS (
                RETURN_ID VARCHAR, RAW_DATA VARIANT,
                INGESTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )""")
        for r in returns:
            hook.run(
                'INSERT INTO RAW.RAW_RETURNS(RETURN_ID, RAW_DATA) '
                'SELECT %s, PARSE_JSON(%s)',
                parameters=(r['return_id'], json.dumps(r)))

    @task()
    def generate_inventory():
        # Daily snapshots — drives SCD on DIM_PRODUCT (stock levels)
        snapshots = [{'snapshot_id': str(uuid.uuid4()),
                      'product_id': random.randint(1,100),
                      'stock_level': random.randint(0,500),
                      'snapshot_date': datetime.today().date().isoformat(),
                      'warehouse_location': random.choice(
                          ['US-WEST','US-EAST','EU-CENTRAL','APAC'])}
                     for _ in range(100)]
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run("""
            CREATE TABLE IF NOT EXISTS RAW.RAW_INVENTORY (
                SNAPSHOT_ID VARCHAR, RAW_DATA VARIANT,
                INGESTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )""")
        for s in snapshots:
            hook.run(
                'INSERT INTO RAW.RAW_INVENTORY(SNAPSHOT_ID, RAW_DATA) '
                'SELECT %s, PARSE_JSON(%s)',
                parameters=(s['snapshot_id'], json.dumps(s)))

    p = ingest_products()
    u = ingest_users()
    o = generate_orders()
    r = generate_returns()
    i = generate_inventory()
    [p, u] >> o >> [r, i]

ingest_dag()

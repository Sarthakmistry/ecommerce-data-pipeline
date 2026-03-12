import json
from datetime import date, datetime
from snowflake.connector import connect
from dotenv import load_dotenv
import os

load_dotenv()


class SafeEncoder(json.JSONEncoder):
    """Handles date/datetime and other non-serializable types from GE results."""
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)  # fallback — stringify anything else


def write_to_quarantine(failed_results: list, source_table: str, dag_run_id: str):
    conn = connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        schema='STAGING'
    )
    cursor = conn.cursor()
    for failure in failed_results:
        cursor.execute('''
            INSERT INTO ECOMMERCE.STAGING.QUARANTINE (
                SOURCE_TABLE, FAILED_EXPECTATION, FAILED_COLUMN,
                RAW_RECORD, FAILURE_DETAILS, DAG_RUN_ID
            )
            SELECT %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), %s
        ''', (
            source_table,
            failure['expectation_type'],
            failure.get('column', 'N/A'),
            json.dumps(failure.get('unexpected_values', []), cls=SafeEncoder),
            json.dumps(failure.get('details', {}), cls=SafeEncoder),
            dag_run_id
        ))
    conn.commit()
    cursor.close()
    conn.close()
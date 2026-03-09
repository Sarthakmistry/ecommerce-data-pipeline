from cosmos import DbtDag, DbtTaskGroup, ProjectConfig
from cosmos import ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import ExecutionMode
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import great_expectations as gx
import os

PROFILE_CONFIG = ProfileConfig(
    profile_name='ecommerce',
    target_name='prod',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id='snowflake_conn',
        profile_args={
            'database': 'ECOMMERCE',
            'schema': 'STAGING'
        }
    )
)

EXECUTION_CONFIG = ExecutionConfig(
    execution_mode=ExecutionMode.VIRTUALENV,
    venv_path='/usr/local/airflow/dbt_venv'
)

PROJECT_CONFIG = ProjectConfig(
    '/usr/local/airflow/include/ecommerce'
)

def run_ge_checkpoint(checkpoint_name: str):
    context = gx.get_context(
        context_root_dir='/usr/local/airflow/include/great_expectations'
    )
    result = context.run_checkpoint(checkpoint_name=checkpoint_name)
    if not result['success']:
        raise ValueError(f'GE checkpoint {checkpoint_name} FAILED')

@dag(
    dag_id='ecomm_transform',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['transform', 'dbt', 'cosmos']
)
def transform_dag():

    ge_raw_check = PythonOperator(
        task_id='ge_validate_raw',
        python_callable=run_ge_checkpoint,
        op_kwargs={'checkpoint_name': 'raw_products_checkpoint'}
    )

    with DbtTaskGroup(
        group_id='staging_models',
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        select=['staging'],
    ) as staging_group:
        pass

    ge_staging_check = PythonOperator(
        task_id='ge_validate_staging',
        python_callable=run_ge_checkpoint,
        op_kwargs={'checkpoint_name': 'staging_products_checkpoint'}
    )

    with DbtTaskGroup(
        group_id='vault_models',
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        select=['vault'],
    ) as vault_group:
        pass

    with DbtTaskGroup(
        group_id='marts_models',
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        select=['marts'],
    ) as marts_group:
        pass

    with DbtTaskGroup(
        group_id='snapshots',
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        dbt_command='dbt snapshot',
    ) as snapshots_group:
        pass

    # Full pipeline dependency chain
    (ge_raw_check
     >> staging_group
     >> ge_staging_check
     >> vault_group
     >> marts_group
     >> snapshots_group)

transform_dag()
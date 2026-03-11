from cosmos import DbtTaskGroup, ProjectConfig
from cosmos import ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import ExecutionMode, LoadMode
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime
import great_expectations as gx
import sys
import os

# Add include/ to path so quarantine_utils is importable
sys.path.insert(0, '/usr/local/airflow/include')
from quarantine_utils import write_to_quarantine

PROFILE_CONFIG = ProfileConfig(
    profile_name="ecommerce",
    target_name="prod",
    profiles_yml_filepath="/usr/local/airflow/include/ecommerce/profiles.yml",
)

EXECUTION_CONFIG = ExecutionConfig(
    execution_mode=ExecutionMode.VIRTUALENV,
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)

PROJECT_CONFIG = ProjectConfig(
    dbt_project_path="/usr/local/airflow/include/ecommerce",
    models_relative_path="models",
    seeds_relative_path="seeds",
    snapshots_relative_path="snapshots",
)

def run_ge_checkpoint(checkpoint_name: str, dag_run_id: str = None):
    context = gx.get_context(
        mode="file",
        project_root_dir='/usr/local/airflow/include/great_expectations'
    )

    result = context.checkpoints.get(checkpoint_name).run()

    if not result.success:
        failed_records = []
        for validation_key, validation_result in result.run_results.items():
            source_table = str(validation_key).split("//")[-1].split(":")[0]
            for r in validation_result.results:
                if not r.success:
                    failure = {
                        'expectation_type': r.expectation_config.expectation_type,
                        'column': r.expectation_config.kwargs.get('column', 'N/A'),
                        'unexpected_values': r.result.get('partial_unexpected_list', []),
                        'details': {
                            'kwargs': r.expectation_config.kwargs,
                            'result': r.result
                        }
                    }
                    failed_records.append(failure)
                    print(f"FAILED: {failure['expectation_type']} "
                          f"on column {failure['column']}")

        if failed_records:
            write_to_quarantine(
                failed_results=failed_records,
                source_table=source_table,
                dag_run_id=dag_run_id or 'unknown'
            )
        print(f"{len(failed_records)} failures written to QUARANTINE. Pipeline continues.")
    else:
        print(f"Checkpoint {checkpoint_name} passed all expectations.")


@dag(
    dag_id='ecomm_transform',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=['transform', 'dbt', 'cosmos']
)
def transform_dag():

    # No raw check — raw is append-only, validation happens at staging
    staging_group = DbtTaskGroup(
        group_id='staging_models',
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            select=['path:models/staging'],
            load_method=LoadMode.DBT_LS,
        ),
    )

    # GE validates staging — failures go to quarantine, pipeline continues
    ge_staging_check = PythonOperator(
        task_id='ge_validate_staging',
        python_callable=run_ge_checkpoint,
        op_kwargs={
            'checkpoint_name': 'staging_checkpoint',
            'dag_run_id': '{{ run_id }}'   # Airflow injects the run ID
        }
    )

    vault_group = DbtTaskGroup(
        group_id='vault_models',
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            select=['path:models/vault'],
            load_method=LoadMode.DBT_LS,
        ),
    )

    snapshots_group = DbtTaskGroup(
        group_id='snapshots',
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:snapshots'],
        ),
    )

    marts_group = DbtTaskGroup(
        group_id='marts_models',
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            select=['path:models/marts'],
            load_method=LoadMode.DBT_LS,
        ),
    )

    (staging_group
     >> ge_staging_check
     >> vault_group
     >> snapshots_group
     >> marts_group)


transform_dag()
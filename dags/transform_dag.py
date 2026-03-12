from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
import great_expectations as gx
import sys
import pendulum
from datetime import timedelta

sys.path.insert(0, '/usr/local/airflow/include')
from quarantine_utils import write_to_quarantine

PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/include/ecommerce',
    models_relative_path='models',
    seeds_relative_path='seeds',
    snapshots_relative_path='snapshots',
)

PROFILE_CONFIG = ProfileConfig(
    profile_name='ecommerce',
    target_name='prod',
    profiles_yml_filepath='/usr/local/airflow/include/ecommerce/profiles.yml',
)

EXECUTION_CONFIG = ExecutionConfig(
    execution_mode=ExecutionMode.VIRTUALENV,
    dbt_executable_path='/usr/local/airflow/dbt_venv/bin/dbt',
)


def run_ge_checkpoint(checkpoint_name: str, dag_run_id: str = None):
    context = gx.get_context(
        mode='file',
        project_root_dir='/usr/local/airflow/include/great_expectations'
    )
    result = context.checkpoints.get(checkpoint_name).run()

    if not result.success:
        failed_records = []
        for validation_key, validation_result in result.run_results.items():
            source_table = str(validation_key).split('//')[-1].split(':')[0]
            for r in validation_result.results:
                if not r.success:
                    failure = {
                        'expectation_type': r.expectation_config.type,
                        'column': r.expectation_config.kwargs.get('column', 'N/A'),
                        'unexpected_values': r.result.get('partial_unexpected_list', []),
                        'details': {
                            'kwargs': r.expectation_config.kwargs,
                            'result': r.result
                        }
                    }
                    failed_records.append(failure)
                    print(f'FAILED: {failure["expectation_type"]} on {failure["column"]}')

        if failed_records:
            write_to_quarantine(
                failed_results=failed_records,
                source_table=source_table,
                dag_run_id=dag_run_id or 'unknown'
            )
            print(f'{len(failed_records)} failures written to QUARANTINE. Pipeline continues.')
    else:
        print(f'Checkpoint {checkpoint_name} passed all expectations.')


@dag(
    dag_id='ecomm_transform',
    schedule_interval='0 23 * * *',
    start_date=pendulum.datetime(2024, 1, 1, tz='America/New_York'),
    catchup=False,
    is_paused_upon_creation=False,
    tags=['transform', 'dbt', 'cosmos']
)
def transform_dag():

    # Staging models
    staging_group = DbtTaskGroup(
        group_id='staging_models',
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            select=['path:models/staging'],
            load_method=LoadMode.DBT_LS
        ),
    )

    # GE staging validation 
    ge_staging_check = PythonOperator(
        task_id='ge_validate_staging',
        python_callable=run_ge_checkpoint,
        op_kwargs={
            'checkpoint_name': 'staging_checkpoint',
            'dag_run_id': '{{ run_id }}'
        }
    )

    # Vault models 
    vault_group = DbtTaskGroup(
        group_id='vault_models',
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            select=['path:models/vault'],
            load_method=LoadMode.DBT_LS
        ),
    )

    # Snapshots
    snapshots_group = DbtTaskGroup(
        group_id='snapshots',
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            select=['path:snapshots'],
            load_method=LoadMode.DBT_LS
        ),
    )

    # Marts models
    marts_group = DbtTaskGroup(
        group_id='marts_models',
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            select=['path:models/marts'],
            load_method=LoadMode.DBT_LS
        ),
    )

    # GE marts validation
    ge_marts_check = PythonOperator(
        task_id='ge_validate_marts',
        python_callable=run_ge_checkpoint,
        op_kwargs={
            'checkpoint_name': 'marts_checkpoint',
            'dag_run_id': '{{ run_id }}'
        }
    )
    
    (
        staging_group
        >> ge_staging_check
        >> vault_group
        >> snapshots_group
        >> marts_group
        >> ge_marts_check
    )


transform_dag()
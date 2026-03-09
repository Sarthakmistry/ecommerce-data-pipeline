FROM quay.io/astronomer/astro-runtime:12.0.0


RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
    dbt-snowflake==1.8.0

ENV DBT_HOME=/usr/local/airflow/include/ecommerce
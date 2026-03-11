# E-Commerce Data Pipeline

Airflow + Cosmos + dbt + Snowflake + Great Expectations

## Prerequisites
- Docker Desktop
- Astro CLI (`brew install astro`)
- Python 3.12+
- Snowflake account (free trial at trial.snowflake.com)

## Setup

### 1. Clone and configure environment
git clone https://github.com/sarthakmistry/ecomm-pipeline
cd ecomm-pipeline
cp .env.example .env
# Edit .env with your Snowflake credentials

### 2. Set up Snowflake
# Run the DDL in snowflake_setup.sql in your Snowflake worksheet

### 3. Configure dbt
cp include/ecommerce/profiles.example.yml include/ecommerce/profiles.yml
# Edit profiles.yml with your Snowflake credentials

### 4. Start Airflow
astro dev start
# Airflow UI: http://localhost:8080 (admin/admin)
# Data Docs: http://localhost:8081

### 5. Run Great Expectations setup
cd include/great_expectations
python ge_setup.py

### 6. Trigger the pipeline
# In Airflow UI: trigger ecomm_ingest, then ecomm_transform
# Or via CLI:
astro dev run dags trigger ecomm_ingest
astro dev run dags trigger ecomm_transform

# E-Commerce Data Pipeline

An end-to-end data platform that ingests product, order, customer, and inventory data from external APIs and synthetic generators, transforms it through a medallion architecture (RAW → Staging → Data Vault 2.0 → Star Schema), validates data quality at every layer, and delivers analytics-ready tables to Snowflake — all orchestrated by Airflow.
## Architecture

```
                         ┌──────────────────────────────────────────────────────────────────────┐
                         │                        SNOWFLAKE · ECOMMERCE DB                      │
                         │                                                                      │
  DummyJSON API ──┐      │   ┌─────────┐    ┌───────────┐    ┌─────────┐    ┌───────────────┐   │
                  ├──►   │   │   RAW   │──► │  STAGING  │──► │  VAULT  │──► │     MARTS     │   │
  Faker Engine ───┘      │   │ VARIANT │    │  cleaned  │    │ DV 2.0  │    │  star schema  │   │
                         │   └─────────┘    └─────┬─────┘    └─────────┘    │  + OBT + RFM  │   │
                         │                        │                         └───────────────┘   │
                         │                        ▼                                │            │
                         │               ┌──────────────┐              ┌───────────┘            │
                         │               │  QUARANTINE  │              ▼                        │ 
                         │               │ failed rows +│       Cortex Analyst /                │
                         │               │ GE metadata  │       Snowflake Intelligence          │
                         │               └──────────────┘                                       │
                         └──────────────────────────────────────────────────────────────────────┘

  Orchestration: Airflow 2.x (Astro CLI) · Astronomer Cosmos 1.7.0
  Transformation: dbt-snowflake 1.8.0 · Data Vault 2.0 → Star Schema
  Data Quality: Great Expectations 1.3.x · 2 checkpoints · 7 expectation suites
```

## Key Components of pipeline

**Data Modeling** — Four-layer medallion architecture progressing from raw VARIANT JSON through staging views, Data Vault 2.0 hubs/satellites/links, to a consumer-facing star schema. The final OBT (One Big Table) adds RFM customer segmentation, product performance tiers, MoM growth, rolling revenue windows, and running aggregates.

**Orchestration** — Two Airflow DAGs managed via Astro CLI and Astronomer Cosmos. The ingest DAG pulls live product and customer data and generates synthetic orders, returns, and inventory snapshots using Faker. The transform DAG sequences dbt model groups with Great Expectations checkpoints inserted between layers, so data quality is validated before downstream models consume upstream output.

**Data Quality** — Great Expectations runs 50+ expectations across 7 suites covering schema validation, freshness checks, statistical distribution guards, and referential integrity. Failures don't halt the pipeline, they're logged to a Snowflake quarantine table with full GE metadata for triage, while the pipeline continues with warnings.

**Data Vault 2.0** — Hubs isolate business keys, satellites track attribute history with hash-diff change detection, and links capture many-to-many relationships. Incremental materialization ensures only new or changed records are loaded on each run.

**SCD Type 2** — dbt snapshots track customer attribute changes over time using a check strategy on email, address, and phone. The mart layer joins to the current snapshot for dimension tables.

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Orchestration | Airflow 2.x + Astro CLI | DAG scheduling, task dependency management |
| dbt Integration | Astronomer Cosmos 1.7.0 | Renders dbt models as native Airflow task groups |
| Transformation | dbt-snowflake 1.8.0 | SQL-based transformations with incremental models |
| Data Warehouse | Snowflake (AWS US East 2) | Storage, compute, and serving |
| Data Quality | Great Expectations 1.3.x | Expectation suites, checkpoints, quarantine routing |
| Synthetic Data | Faker + DummyJSON API | Realistic e-commerce test data generation |
| Containerization | Docker (Astro Runtime 12.0) | Reproducible local development environment |

## Project Structure

```
├── dags/
│   ├── ingest_dag.py              # API ingestion + synthetic data generation
│   └── transform_dag.py           # dbt staging → vault → marts with GE gates
├── include/
│   ├── ecommerce/                 # dbt project (see include/ecommerce/README.md)
│   │   ├── models/
│   │   │   ├── staging/           # 4 views — flatten VARIANT, deduplicate
│   │   │   ├── vault/             # 3 hubs, 2 satellites, 1 link (incremental)
│   │   │   └── marts/             # 3 dims, 3 facts, 1 OBT (tables)
│   │   └── snapshots/             # SCD Type 2 on customers
│   ├── great_expectations/        # GE project (see include/great_expectations/README.md)
│   │   ├── ge_setup.py            # Suite definitions + checkpoint registration
│   │   └── gx/                    # GE context, checkpoints, validation configs
│   └── quarantine_utils.py        # Writes GE failures to Snowflake quarantine table
├── Dockerfile                     # Astro Runtime + isolated dbt venv
├── requirements.txt               # Python dependencies
└── .env.example                   # Snowflake credential template
```

## Getting Started

**Prerequisites:** Docker Desktop, Astro CLI (`brew install astro`), a Snowflake account, Python 3.12+.

**1. Clone and configure**

```bash
git clone https://github.com/sarthakmistry/ecomm-pipeline
cd ecomm-pipeline
cp .env.example .env
# Fill in your Snowflake credentials in .env
```

**2. Configure dbt**

```bash
cp include/ecommerce/profiles.example.yml include/ecommerce/profiles.yml
# Fill in your Snowflake credentials in profiles.yml
```

**3. Start Airflow**

```bash
astro dev start
# Airflow UI → http://localhost:8080 (admin / admin)
```

**4. Initialize Great Expectations**

```bash
cd include/great_expectations
python ge_setup.py
# Opens Data Docs in your browser automatically
```

**5. Run the pipeline**

Trigger the DAGs in order — `ecomm_ingest` first, then `ecomm_transform` — either from the Airflow UI or via CLI:

```bash
astro dev run dags trigger ecomm_ingest
# Wait for completion, then:
astro dev run dags trigger ecomm_transform
```

## Design Decisions

**Why Data Vault 2.0 as the integration layer?** A star schema is the right shape for analytics consumption, but loading directly from staging to star schema makes historical tracking and source auditability difficult. The vault layer provides a load-date-stamped, hash-keyed, insert-only audit trail that cleanly separates business keys (hubs) from descriptive attributes (satellites) from relationships (links). The marts layer then reads from the vault to build consumer-friendly dimensions and facts.

**Why quarantine instead of hard-fail on data quality?** In a daily batch pipeline processing synthetic data, some quality issues (null brands from DummyJSON, VARIANT case sensitivity mismatches) are known source limitations, not pipeline bugs. Hard-failing would block the entire pipeline for expected issues. Instead, failures are routed to a quarantine table with full metadata — the pipeline continues, and the quarantine table serves as an audit log for triage.

**Why Cosmos for dbt?** Running dbt as a single BashOperator in Airflow treats the entire dbt run as one opaque task. Cosmos renders each dbt model as an individual Airflow task, giving per-model visibility in the Airflow graph view, per-model retries, and the ability to insert non-dbt tasks (like GE checkpoints) between dbt model groups.


**Why an isolated dbt virtualenv in Docker?** dbt and Airflow share Python dependencies, and version conflicts between dbt-snowflake's dependency tree and Airflow's providers are common. The Dockerfile creates a separate venv for dbt (`/usr/local/airflow/dbt_venv`) so the two environments don't interfere with each other. Cosmos's `VIRTUALENV` execution mode points to this path.

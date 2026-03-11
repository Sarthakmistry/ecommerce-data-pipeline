# ecommerce — dbt Project

This dbt project transforms raw e-commerce data ingested by Airflow into analytics-ready star schema via a Data Vault 2.0 integration layer. It models  e-commerce data covering products, customers, orders, returns, and inventory.

---

## Workflow Overview

```
ECOMMERCE.RAW  (Airflow ingests here)
      ↓
ECOMMERCE.STAGING  (dbt staging models — clean, typed, deduplicated views)
      ↓
ECOMMERCE.VAULT  (dbt vault models — Data Vault 2.0: Hubs, Links, Satellites)
      ↓
ECOMMERCE.MARTS  (dbt snapshot — SCD Type 2)
      ↓
ECOMMERCE.MARTS  (dbt marts models — Star Schema: Dimensions + Facts)
```

---

## dbt Run Commands

Always run layers in order. Each layer depends on the one above it.

```bash
# Navigate to dbt project root
cd include/ecommerce

# Verify Snowflake connection
dbt debug

# Run individual layers in order
dbt run --select staging
dbt run --select vault
dbt snapshot
dbt run --select marts

# Run all models at once (dbt resolves dependency order automatically)
dbt run

# Run tests
dbt test

# Generate and serve dbt documentation
dbt docs generate
dbt docs serve
```

---

## Layer 1 — Staging Models (`models/staging/`)

Materialized as **views** in `ECOMMERCE.STAGING`. Each model reads from raw Snowflake VARIANT JSON, flattens into typed columns, and deduplicates using `ROW_NUMBER()`

---

## Layer 2 — Data Vault Models (`models/vault/`)

Materialized as **incremental tables** in `ECOMMERCE.VAULT`. Implements Data Vault 2.0 with MD5 hash keys. Only new or changed records are inserted on each run.

### Hubs — Business Keys

### Satellites — Descriptive Attributes

### Links — Relationships

---

## Layer 3 — Snapshot (`snapshots/`)

Implements **SCD Type 2** using dbt's native snapshot functionality. Tracks historical changes to customer attributes over time.

dbt automatically adds these columns to every snapshot:

| Column | Description |
|---|---|
| `DBT_VALID_FROM` | When this version of the record became active |
| `DBT_VALID_TO` | When superseded — `NULL` means current record |
| `DBT_SCD_ID` | Unique identifier for each historical row |
| `DBT_UPDATED_AT` | Timestamp of last change |

> **Important:** Snapshots must run **before** marts becuase of dependency

---

## Layer 4 — Marts Models (`models/marts/`)

Materialized as **tables** in `ECOMMERCE.MARTS`. Implements a star schema built on top of the vault layer. Consumer-ready for Snowflake dashboards and Snowsight.


## Full Model Dependency Graph

```
RAW.RAW_PRODUCTS ──► stg_products ──► hub_product ──► sat_product_details ──► dim_product ──────────────────┐
                                                                                                              │
RAW.RAW_CUSTOMERS ──► stg_customers ──► hub_customer ──► sat_customer_details ──► dim_customer               │
                                  └───► customer_snapshot (SCD Type 2) ─────────────────────────────────┐   │
                                                                                                         ▼   ▼
RAW.RAW_ORDERS ──► stg_orders ──────────────────────────────────────────────────────────────────────► fact_orders
                          └──────────────────────────────────────────────────────────────────────────► lnk_order_product

RAW.RAW_RETURNS ──► stg_returns ──────────────────────────────────────────────────────────────────────► fact_orders

dim_date ─────────────────────────────────────────────────────────────────────────────────────────────► fact_orders
```

---

## Schema Routing

Schemas are controlled by `dbt_project.yml` and a custom `generate_schema_name` macro in `macros/generate_schema_name.sql`. Without this macro, dbt prefixes the profile's default schema to every custom schema (creating `RAW_STAGING` instead of `STAGING`).

---

## Profiles

`profiles.yml` is gitignored. Copy the example and fill in your Snowflake credentials:

```bash
cp profiles.example.yml profiles.yml
```

The `schema` field in `profiles.yml` acts only as a fallback default — it does not affect where models land because all layers have explicit `+schema` defined in `dbt_project.yml`.
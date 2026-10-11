# Crypto Lakehouse

A crypto market lakehouse built from scratch on **Databricks** (Free Edition), using the **medallion architecture**, **Delta Lake** and **Unity Catalog**. Built module by module as a hands-on study project: every notebook explains the concept it applies.

## Architecture

```
 Crypto API ──► LANDING (UC Volume, raw JSON files, never edited)
                   │  Auto Loader (incremental, checkpointed)
                   ▼
                BRONZE (Delta: raw data + ingestion metadata)
                   │  parse, type, dedup, MERGE, SCD2, quality checks
                   ▼
                SILVER (Delta: clean entities)
                   │  dimensional model
                   ▼
                GOLD   (Delta: star schema, KPIs) ──► SQL Warehouse / dashboard
```

## Modules

| # | Notebook | What it builds | Concepts |
|---|---|---|---|
| 0 | `notebooks/00_setup` | Schemas per layer, volumes, API check | Unity Catalog, volumes, idempotency, Delta versions |
| 1 | `notebooks/01_extract_api` | API extractor → landing | Python OOP (ABC, inheritance), decorators, dataclasses |
| 2 | `notebooks/02_bronze` | Landing → bronze | Auto Loader, checkpoints, `availableNow`, schema evolution |
| 3 | `notebooks/03_silver` | Bronze → silver | window dedup, MERGE, SCD Type 2, quarantine |
| 4 | `notebooks/04_gold` | Silver → gold | star schema, grain, surrogate keys, SQL windows |
| 5 | `notebooks/05_delta_deep_dive` | Delta internals | transaction log, time travel, CDF, OPTIMIZE, liquid clustering, VACUUM |
| 6 | `notebooks/06_spark_performance` | Performance lab | shuffle, broadcast vs sort-merge, skew, AQE, UDFs |
| 7 | `notebooks/07_orchestration` | Job + Airflow DAG | Lakeflow Jobs, Airflow equivalents |

## How to run

1. In Databricks, create a **Git folder** from this repository (Workspace → Create → Git folder).
2. Open `notebooks/00_setup`, attach **Serverless** compute and run all.
3. Continue with the modules in order. All names and paths live in `notebooks/config`.

## Tech

Databricks (serverless) · PySpark · Delta Lake · Unity Catalog · Auto Loader · Python

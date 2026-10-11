# Databricks notebook source
# MAGIC %md
# MAGIC # Module 0 — Setup and environment check
# MAGIC
# MAGIC **Project:** a crypto market lakehouse built from scratch on Databricks, one module at a time.
# MAGIC
# MAGIC ```
# MAGIC  Crypto API ──► LANDING (Volume, raw JSON files)
# MAGIC                    │  Auto Loader
# MAGIC                    ▼
# MAGIC                 BRONZE (Delta: raw + ingestion metadata)
# MAGIC                    │  parse, type, dedup, MERGE, SCD2, quality
# MAGIC                    ▼
# MAGIC                 SILVER (Delta: clean entities)
# MAGIC                    │  dimensional model
# MAGIC                    ▼
# MAGIC                 GOLD   (Delta: star schema, KPIs) ──► SQL Warehouse / dashboard
# MAGIC ```
# MAGIC
# MAGIC ### How every notebook is organized
# MAGIC | Block | What it is |
# MAGIC |---|---|
# MAGIC | **INTERVIEW CONCEPT** | The theory, in plain words, right where it's used |
# MAGIC | **SEE IT** | A cell you run to *watch* the concept happen; look at the output |
# MAGIC | **SAY IT** | The English answer to practice out loud |
# MAGIC
# MAGIC **Concepts in this module:** Unity Catalog (catalog → schema → volume), medallion layers,
# MAGIC serverless compute, driver vs executors, idempotency, Delta versions.
# MAGIC
# MAGIC **How to run:** attach to *Serverless* and run all cells top to bottom.
# MAGIC Send me the output of **Step 4** (API check) and any error you hit.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Where does my code run?
# MAGIC
# MAGIC > **INTERVIEW CONCEPT — Driver vs executors.**
# MAGIC > The **driver** runs your Python, builds the plan and schedules work. **Executors** run the tasks on the data.
# MAGIC > Plain Python (`print`, `requests`, a `for` loop) runs only on the driver. Spark DataFrame operations are
# MAGIC > split into tasks and run on executors. On **serverless**, Databricks manages those machines for you.
# MAGIC
# MAGIC **SEE IT:** the cell below is plain Python → it runs on the driver. The `spark.range(...).count()` line is a
# MAGIC Spark **action** → it becomes a job with tasks on executors (click *Spark Jobs* under the cell to see it).

# COMMAND ----------

print("Spark version  :", spark.version)
print("Current user   :", spark.sql("SELECT current_user()").first()[0])
print("Current catalog:", spark.sql("SELECT current_catalog()").first()[0])

# A Spark action: creates a job, split into tasks that run in parallel
print("Rows counted by executors:", spark.range(0, 10_000_000).count())

# COMMAND ----------

# MAGIC %md
# MAGIC > **SAY IT:** *"The driver runs my program and plans the work; executors run tasks in parallel, one per partition.
# MAGIC > That's why I never `collect()` large data — it pulls everything onto the driver."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — One schema per medallion layer
# MAGIC
# MAGIC > **INTERVIEW CONCEPT — Unity Catalog hierarchy.**
# MAGIC > Every object has a three-level name: `catalog.schema.table`. Here: `workspace.crypto_bronze.markets`.
# MAGIC > We create **one schema per layer** so that, in a real company, permissions are granted per layer:
# MAGIC > engineers write bronze/silver, analysts read only gold (`GRANT SELECT ON SCHEMA ... TO analysts`).
# MAGIC
# MAGIC > **INTERVIEW CONCEPT — Idempotency.**
# MAGIC > `IF NOT EXISTS` means running the cell twice gives the same result. Every step in this project is built
# MAGIC > to be safely re-runnable — that's what makes retries and backfills safe in production.
# MAGIC
# MAGIC **SEE IT:** run the cell, then run it again. Nothing breaks, nothing duplicates.

# COMMAND ----------

for schema in [SCHEMA_LANDING, SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print("schema ready:", f"{CATALOG}.{schema}")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG} LIKE 'crypto*'"))

# COMMAND ----------

# MAGIC %md
# MAGIC > **SAY IT:** *"I organize Unity Catalog with a schema per layer, so access is granted by layer: engineers
# MAGIC > write to bronze and silver, consumers only read gold. And every step is idempotent, so reruns are safe."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Volumes: the landing zone
# MAGIC
# MAGIC > **INTERVIEW CONCEPT — Volume and landing zone.**
# MAGIC > A **volume** is a Unity Catalog object for files that are not tables (JSON, CSV, binaries), governed like tables.
# MAGIC > The **landing zone** keeps source files *exactly as received*, never edited. If a bug appears in silver,
# MAGIC > we reprocess from these files instead of calling the API again.
# MAGIC > The `checkpoints` volume is where Auto Loader remembers which files it already ingested (Module 2).
# MAGIC
# MAGIC **SEE IT:** after running, open **Catalog** (left menu) → `workspace` → `crypto_landing` → *Volumes*.

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_LANDING}.{LANDING_VOLUME}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_LANDING}.{CHECKPOINT_VOLUME}")

# Volumes behave like folders: normal Python file APIs work on /Volumes/...
import os
os.makedirs(f"{LANDING_PATH}/coingecko/markets", exist_ok=True)
print("landing folder ready:", f"{LANDING_PATH}/coingecko/markets")
print("checkpoint folder   :", CHECKPOINT_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC > **SAY IT:** *"Raw files land untouched in a Unity Catalog volume. That gives me replay: I can rebuild bronze,
# MAGIC > silver and gold from the original files without going back to the source."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Which crypto APIs can this workspace reach?
# MAGIC
# MAGIC Free Edition restricts outbound internet to trusted domains, so we test before building.
# MAGIC - `HTTP 200` → reachable from Databricks.
# MAGIC - `FAILED` / timeout / `451` → blocked; we'd fetch from your computer and upload the files to the volume
# MAGIC   (a realistic pattern too: many sources *push* files to a landing zone).

# COMMAND ----------

import requests

ENDPOINTS = {
    "coingecko": "https://api.coingecko.com/api/v3/ping",
    "binance":   "https://api.binance.com/api/v3/ping",
    "coinbase":  "https://api.exchange.coinbase.com/products/BTC-USD/ticker",
    "kraken":    "https://api.kraken.com/0/public/Time",
}

for name, url in ENDPOINTS.items():
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "crypto-lakehouse-study/1.0"})
        status = f"HTTP {r.status_code}"
    except Exception as e:
        status = f"FAILED: {type(e).__name__}: {str(e)[:100]}"
    print(f"{name:10} -> {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — First look at Delta
# MAGIC
# MAGIC > **INTERVIEW CONCEPT — Delta table and transaction log.**
# MAGIC > A Delta table = Parquet files + a transaction log (`_delta_log`). Every write is an atomic **commit** that
# MAGIC > creates a new **version**. That log is what gives ACID and time travel.
# MAGIC
# MAGIC **SEE IT:** two writes → two versions in `DESCRIBE HISTORY`. Then we read **version 0** (time travel):
# MAGIC it has only the first row.

# COMMAND ----------

test_table = table(SCHEMA_BRONZE, "_smoke_test")

spark.createDataFrame([(1, "hello")], "id INT, msg STRING").write.mode("overwrite").saveAsTable(test_table)
spark.createDataFrame([(2, "lakehouse")], "id INT, msg STRING").write.mode("append").saveAsTable(test_table)

print("Current version:")
display(spark.table(test_table))

print("History (one row per commit):")
display(spark.sql(f"DESCRIBE HISTORY {test_table}").select("version", "timestamp", "operation"))

print("Time travel to version 0:")
display(spark.sql(f"SELECT * FROM {test_table} VERSION AS OF 0"))

# COMMAND ----------

# MAGIC %md
# MAGIC > **SAY IT:** *"Delta stores Parquet files plus a transaction log. Each write is an atomic commit and a new version,
# MAGIC > so readers never see half a write, and I can time travel to any version until VACUUM removes old files."*

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {test_table}")
print("Setup complete. Send me the Step 4 output and we move to Module 1.")
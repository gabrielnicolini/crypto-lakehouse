# Databricks notebook source
# MAGIC %md
# MAGIC # Project config
# MAGIC
# MAGIC Shared settings for every module. Each notebook loads this file with `%run ./config`,
# MAGIC so names and paths live in **one place** (if you change a schema name, you change it here only).

# COMMAND ----------

# Unity Catalog: catalog -> schema -> table/volume
# Free Edition ships with a catalog called "workspace"; we create one schema per medallion layer.
CATALOG = "workspace"

SCHEMA_LANDING = "crypto_landing"   # volumes with raw files exactly as received
SCHEMA_BRONZE  = "crypto_bronze"    # raw data as Delta tables + ingestion metadata
SCHEMA_SILVER  = "crypto_silver"    # cleaned, typed, deduplicated
SCHEMA_GOLD    = "crypto_gold"      # star schema / business-ready

# Volumes (Unity Catalog objects for non-tabular files)
LANDING_VOLUME     = "raw"
CHECKPOINT_VOLUME  = "checkpoints"

LANDING_PATH    = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}/{LANDING_VOLUME}"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}/{CHECKPOINT_VOLUME}"

# Coins we will track (CoinGecko ids)
COINS = ["bitcoin", "ethereum", "solana", "cardano", "chainlink"]
VS_CURRENCY = "usd"

def table(schema: str, name: str) -> str:
    """Return the full three-level name, e.g. workspace.crypto_bronze.markets."""
    return f"{CATALOG}.{schema}.{name}"

print(f"Config loaded | catalog={CATALOG} | landing={LANDING_PATH}")

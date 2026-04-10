# Databricks notebook source
"""
config.py — Centralized configuration for the Bike Store ETL Pipeline.

All Azure storage paths, secret retrieval, and Spark session configuration
are managed here. Import this module in every notebook to avoid
duplicating credentials or path definitions.

Usage (in other Databricks notebooks):
    # MAGIC %run ./config
"""

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = SparkSession.builder.appName("bike_store_etl").getOrCreate()

# ---------------------------------------------------------------------------
# Azure Storage — Secret-Based Authentication
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT = "adlsbikestoreinterns"
ACCOUNT_URL = f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net"

bikes_account_key = dbutils.secrets.get(scope="bikes-scope", key="account-key")
spark.conf.set(ACCOUNT_URL, bikes_account_key)

# ---------------------------------------------------------------------------
# Data Lake Paths  (Medallion Architecture)
# ---------------------------------------------------------------------------
# Landing Zone — raw ingested files (Parquet)
LANDING_PATH = f"wasbs://dlbikestorelanding@{STORAGE_ACCOUNT}.blob.core.windows.net/Waheed_Landing"
LANDING_DATA_PATH = f"{LANDING_PATH}/Landing"
METADATA_PATH = f"{LANDING_PATH}/metadata/"

# Staging Layer 1 — cleansed data (Delta)
STAGE1_PATH = f"wasbs://dlbikestorestage1@{STORAGE_ACCOUNT}.blob.core.windows.net/Waheed/staging_1"

# Staging Layer 2 — lookup / reference tables (Delta)
STAGE2_PATH = f"wasbs://dlbikestorestage2@{STORAGE_ACCOUNT}.blob.core.windows.net/Waheed/staging_2"

# Info Mart — dimensional model: facts & dimensions (Delta)
INFOMART_PATH = f"wasbs://dlbikestoreinfomart@{STORAGE_ACCOUNT}.blob.core.windows.net/Waheed"

# Data Mart — final flattened aggregation layer (Delta)
DATAMART_PATH = f"wasbs://waheeddatamart@{STORAGE_ACCOUNT}.blob.core.windows.net/sales_datamart"

# ---------------------------------------------------------------------------
# Metadata — loaded once, reused across notebooks
# ---------------------------------------------------------------------------
metadata = spark.read.csv(METADATA_PATH, header=True, inferSchema=True)

# Databricks notebook source
"""
utils.py — Reusable utility functions for the Bike Store ETL Pipeline.

Provides helpers for:
  • Loading and casting source data from the Landing Zone
  • Data profiling (schema, stats, nulls, duplicates, distributions)
  • Data cleansing (nulls, duplicates, outliers, string normalisation)
  • Primary-key validation
  • Zip-code validation via pgeocode

All functions expect PySpark DataFrames and a shared `metadata` DataFrame
that describes column types, nullability, uniqueness, and primary-key flags.
"""

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum, pandas_udf
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, FloatType, DoubleType, LongType, DateType,
)
import matplotlib.pyplot as plt
import numpy as np
import re
import pandas as pd
import pgeocode

nomi = pgeocode.Nominatim("us")

# ═══════════════════════════════════════════════════════════════════════════
#  DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════

def load_and_cast_file_data(spark, metadata_df):
    """Load parquet files from the Landing Zone and cast columns per metadata.

    Parameters
    ----------
    spark : SparkSession
    metadata_df : DataFrame
        Must contain columns: ``destination_filename``, ``source_table``,
        ``table_columns``, ``datatype``.

    Returns
    -------
    dict[str, DataFrame]
        Mapping of table name → DataFrame. Each DF is also injected into
        ``globals()`` as ``<table_name>_df`` for notebook convenience.
    """
    loaded_dataframes = {}

    folder_names = (
        metadata_df.select("destination_filename")
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    for folder_name in folder_names:
        file_path = f"{LANDING_DATA_PATH}/{folder_name}/"
        df = spark.read.parquet(file_path, inferSchema=True)
        loaded_dataframes[folder_name] = df

    for row in metadata_df.collect():
        table_name = row["source_table"]
        column_name = row["table_columns"]
        data_type = row["datatype"]

        df = loaded_dataframes.get(table_name)
        if df is None:
            continue

        type_map = {
            "int": IntegerType(),
            "string": StringType(),
            "float": DoubleType(),
            "datetime": DateType(),
        }
        target_type = type_map.get(data_type)
        if target_type:
            df = df.withColumn(column_name, col(column_name).cast(target_type))

        loaded_dataframes[table_name] = df

    for source_filename, df in loaded_dataframes.items():
        globals()[f"{source_filename}_df"] = df

    return loaded_dataframes


# ═══════════════════════════════════════════════════════════════════════════
#  DATA PROFILING
# ═══════════════════════════════════════════════════════════════════════════

def profile_data(df: DataFrame, df_name: str = "DataFrame"):
    """Print a comprehensive profile of *df* covering schema, stats,
    nulls, duplicates, and value distributions with histograms.
    """
    print(f"Profiling for {df_name}")
    print("-" * 50)

    # Schema
    print("Schema:")
    df.printSchema()

    # Statistical summary
    print("Statistical Summary:")
    print(df.describe().toPandas())

    # Null counts
    print("\nNull Values:")
    null_counts = df.select(
        [_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
    )
    null_counts.show()

    # Duplicates
    duplicates = df.groupBy(df.columns).count().filter("count > 1").count()
    print(f"Duplicate records: {duplicates}\n")

    # Value distributions
    for col_name in df.columns:
        if df.schema[col_name].dataType == StringType():
            print(f"Distribution — {col_name}:")
            df.groupBy(col_name).count().orderBy(col_name).show(10)
        else:
            print(f"Summary Statistics — {col_name}:")
            df.select(col_name).describe().show()
            data = df.select(col_name).rdd.flatMap(lambda x: x).collect()
            plt.figure(figsize=(10, 4))
            plt.hist(data, bins=30, color="#34495e", edgecolor="black")
            plt.title(f"Histogram of {col_name}")
            plt.xlabel(col_name)
            plt.ylabel("Frequency")
            plt.grid(True)
            plt.show()

    print("Profiling Complete")
    print("-" * 50)


# ═══════════════════════════════════════════════════════════════════════════
#  DATA CLEANSING
# ═══════════════════════════════════════════════════════════════════════════

def handle_nulls(df, metadata_df, table_name, flag_values=None):
    """Replace flag values with ``None`` then drop/fill nulls per metadata.

    - Columns marked ``nullable = 'Y'`` → fill with ``'unknown'``.
    - Columns marked ``nullable = 'N'`` → drop rows containing nulls.
    """
    if flag_values is None:
        flag_values = ["unknown", "NA", "NAN", "NULL"]

    df = df.replace(flag_values, None)

    table_metadata = metadata_df.filter(F.col("source_table") == table_name).collect()

    for row in table_metadata:
        column_name = row["table_columns"]
        nullable = row["nullable"]

        if nullable == "Y":
            df = df.na.fill({column_name: "unknown"})
        elif nullable == "N":
            df = df.na.drop(subset=[column_name])

    return df


def handle_duplicates(df, metadata_df, table_name):
    """Drop duplicate rows on columns flagged ``unique = 'Y'`` in metadata."""
    table_metadata = metadata_df.filter(F.col("source_table") == table_name).collect()

    for row in table_metadata:
        column_name = row["table_columns"]
        unique = row["unique"]

        if unique == "Y":
            df = df.drop_duplicates(subset=[column_name])

    return df


def handle_outliers(df, column, method="iqr", strategy="remove", z_thresh=3):
    """Remove or cap outliers using IQR or Z-Score methods.

    Parameters
    ----------
    df : DataFrame
    column : str
    method : {'iqr', 'zscore'}
    strategy : {'remove', 'cap'}
    z_thresh : float
        Z-score threshold (only used when *method* is ``'zscore'``).
    """
    method = method.lower()

    if method == "iqr":
        Q1, Q3 = df.approxQuantile(column, [0.25, 0.75], 0)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        if strategy == "remove":
            return df.filter(
                (F.col(column) >= lower_bound) & (F.col(column) <= upper_bound)
            )
        elif strategy == "cap":
            return df.withColumn(
                column,
                F.when(F.col(column) < lower_bound, lower_bound)
                .when(F.col(column) > upper_bound, upper_bound)
                .otherwise(F.col(column)),
            )

    elif method in ("zscore", "z-score"):
        mean_val = df.select(F.mean(column)).collect()[0][0]
        std_dev = df.select(F.stddev(column)).collect()[0][0]

        df = df.withColumn("z_score", (F.col(column) - mean_val) / std_dev)

        if strategy == "remove":
            return df.filter(F.abs(F.col("z_score")) < z_thresh).drop("z_score")
        elif strategy == "cap":
            lower_bound = mean_val - z_thresh * std_dev
            upper_bound = mean_val + z_thresh * std_dev
            return df.withColumn(
                column,
                F.when(F.col(column) < lower_bound, lower_bound)
                .when(F.col(column) > upper_bound, upper_bound)
                .otherwise(F.col(column)),
            ).drop("z_score")

    raise ValueError("Method not recognised. Use 'iqr' or 'zscore'.")


# ═══════════════════════════════════════════════════════════════════════════
#  STRING NORMALISATION
# ═══════════════════════════════════════════════════════════════════════════

def normalize_strings(dataframes, metadata_df):
    """Trim, lower-case, and strip special characters from string columns
    that match e-mail, phone, or zip-code naming patterns.
    """
    email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    phone_pattern = r"^\d{3}[-.]?\d{3}[-.]?\d{4}$"
    zip_code_pattern = r"^\d{5}(-\d{4})?$"

    for row in metadata_df.collect():
        table_name = row["source_table"]
        column_name = row["table_columns"]

        if any(
            re.search(p, column_name)
            for p in (email_pattern, phone_pattern, zip_code_pattern)
        ):
            df = dataframes.get(table_name)
            if df is None:
                continue

            df = df.withColumn(
                column_name,
                F.trim(
                    F.lower(F.regexp_replace(F.col(column_name), r"[^\w\s]", ""))
                ),
            )

            if re.search(email_pattern, column_name):
                pattern = email_pattern
            elif re.search(phone_pattern, column_name):
                pattern = phone_pattern
            else:
                pattern = zip_code_pattern

            df = df.filter(F.col(column_name).rlike(pattern))
            dataframes[table_name] = df

    return dataframes


# ═══════════════════════════════════════════════════════════════════════════
#  TYPE CASTING
# ═══════════════════════════════════════════════════════════════════════════

def cast_columns_for_all_tables(dataframes, metadata_df):
    """Re-cast column types across all tables based on metadata definitions."""
    type_map = {
        "int": IntegerType(),
        "string": StringType(),
        "float": DoubleType(),
    }

    for row in metadata_df.collect():
        table_name = row["source_table"]
        column_name = row["table_columns"]
        data_type = row["datatype"]

        df = dataframes.get(table_name)
        if df is None:
            continue

        target_type = type_map.get(data_type)
        if target_type:
            df = df.withColumn(column_name, col(column_name).cast(target_type))
        dataframes[table_name] = df

    for source_filename, df in dataframes.items():
        globals()[f"{source_filename}_df"] = df

    return dataframes


# ═══════════════════════════════════════════════════════════════════════════
#  PRIMARY KEY VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

def validate_primary_keys(dataframes, metadata_df):
    """Validate every column flagged ``primary_key = 'Y'`` in metadata."""
    pk_rows = metadata_df.filter(metadata_df["primary_key"] == "Y").collect()

    for row in pk_rows:
        table_name = row["source_table"]
        pk_column = row["table_columns"]
        expected_type = row["datatype"]

        df = dataframes.get(table_name)
        if df is None:
            print(f"Table '{table_name}' not found in dataframes.")
            continue

        if not _validate_single_pk(df, pk_column, expected_type, table_name):
            print(
                f"Primary key validation failed for '{pk_column}' "
                f"in table '{table_name}'."
            )

    return True


def _validate_single_pk(df, pk_column, expected_type, table_name):
    """Check a single primary-key column for nulls, duplicates, and type."""
    if df.filter(F.col(pk_column).isNull()).count() > 0:
        print(f"Table '{table_name}': PK '{pk_column}' contains null values.")
        return False

    if df.groupBy(pk_column).count().filter("count > 1").count() > 0:
        print(f"Table '{table_name}': PK '{pk_column}' contains duplicates.")
        return False

    cast_type = {"int": "int", "float": "float", "string": "string"}.get(expected_type)
    if cast_type:
        bad_rows = df.filter(~F.col(pk_column).cast(cast_type).isNotNull())
        if bad_rows.count() > 0:
            print(
                f"Table '{table_name}': PK '{pk_column}' contains values "
                f"not of type '{expected_type}'."
            )
            return False

    return True


# ═══════════════════════════════════════════════════════════════════════════
#  ZIP CODE VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

def handle_zip_codes(df, column_name):
    """Validate US zip codes using pgeocode and filter invalid rows."""
    if column_name != "zip_code":
        return df

    @pandas_udf("boolean")
    def validate_zip_code(zip_codes: pd.Series) -> pd.Series:
        return zip_codes.apply(
            lambda x: (
                not pd.isna(nomi.query_postal_code(x)["country_code"])
                and nomi.query_postal_code(x)["country_code"] == "US"
            )
        )

    df = (
        df.withColumn("is_valid_zip", validate_zip_code(df[column_name]))
        .filter("is_valid_zip = true")
        .drop("is_valid_zip")
    )
    return df
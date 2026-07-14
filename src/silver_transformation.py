# Databricks notebook source
# Silver layer: clean, validate, and standardize the raw bronze data.
# The core cleaning logic lives in `clean_sales_df` below so it can be
# unit-tested locally (see tests/test_silver_transformation.py) without
# needing a real Databricks job to run.
# Testing Git folder UI commit
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_sales_df(df: DataFrame) -> DataFrame:
    """Business logic for the Bronze -> Silver transformation.

    - Drops rows with a null order_date (can't bucket them by day/month).
    - Fills a missing quantity with 1 (assume a single unit if not recorded).
    - Casts amount to a proper decimal and drops negative/nonsensical rows.
    - Adds an `order_month` column used later for Gold aggregation.
    """
    cleaned = (
        df.filter(F.col("order_date").isNotNull())
        .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(1)))
        .withColumn("amount", F.col("amount").cast("decimal(10,2)"))
        .filter(F.col("amount") >= 0)
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
        .dropDuplicates(["order_id"])
    )
    return cleaned


# COMMAND ----------

# The rest of this cell only runs inside an actual Databricks notebook/job.
# It's guarded so `from silver_transformation import clean_sales_df` works
# fine in pytest, locally, with no dbutils available.

try:
    dbutils  # noqa: F821 - injected by the Databricks runtime
    _IN_DATABRICKS = True
except NameError:
    _IN_DATABRICKS = False

if _IN_DATABRICKS:
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema", "sales_dev")

    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")

# COMMAND ----------

    bronze_df = spark.table(f"{catalog}.{schema}.bronze_sales")
    silver_df = clean_sales_df(bronze_df)

    (
        silver_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.silver_sales")
    )

    print(f"Silver table written: {catalog}.{schema}.silver_sales ({silver_df.count()} rows)")

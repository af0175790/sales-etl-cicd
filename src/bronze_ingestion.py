# Databricks notebook source
# Bronze layer: raw ingestion, no transformation, just land the data as-is.

try:
    dbutils  # noqa: F821 - injected by the Databricks runtime
    _IN_DATABRICKS = True
except NameError:
    _IN_DATABRICKS = False

if _IN_DATABRICKS:
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema", "sales_dev")
    dbutils.widgets.text("raw_path", "/Volumes/workspace/sales_dev/raw_files/sales_raw_sample.csv")

    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    raw_path = dbutils.widgets.get("raw_path")

    # COMMAND ----------

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(raw_path)
    )

    (
        raw_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.bronze_sales")
    )

    print(f"Bronze table written: {catalog}.{schema}.bronze_sales ({raw_df.count()} rows)")

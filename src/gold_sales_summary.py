# Databricks notebook source
# Gold layer: business-ready aggregate — monthly revenue by platform and city.

from pyspark.sql import functions as F

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

    # Testing CI/CD pipeline trigger
    # COMMAND ----------

    silver_df = spark.table(f"{catalog}.{schema}.silver_sales")

    gold_df = (
        silver_df.groupBy("order_month", "platform", "city")
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.sum("quantity").alias("total_units"),
            F.countDistinct("order_id").alias("total_orders"),
        )
        .orderBy("order_month", "platform", "city")
    )

    (
        gold_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.gold_sales_summary")
    )

    print(f"Gold table written: {catalog}.{schema}.gold_sales_summary ({gold_df.count()} rows)")
    display(gold_df)

# COMMAND ----------

    # New: platform-wise total revenue summary (across all cities)
    platform_summary_df = (
        silver_df.groupBy("platform")
        .agg(
            F.sum("amount").alias("platform_total_revenue"),
            F.countDistinct("order_id").alias("platform_total_orders"),
        )
        .orderBy(F.desc("platform_total_revenue"))
    )

    (
        platform_summary_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.gold_platform_summary")
    )

    print(f"Platform summary table written: {catalog}.{schema}.gold_platform_summary")
    display(platform_summary_df)
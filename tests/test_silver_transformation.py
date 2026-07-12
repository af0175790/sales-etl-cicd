"""
Unit tests for the Silver transformation logic.

These run with a local PySpark session — no Databricks workspace needed.
This is exactly what the CI step runs on every push/PR, BEFORE anything
is deployed to Databricks.
"""
import sys
import os
import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from silver_transformation import clean_sales_df  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-silver")
        .getOrCreate()
    )


def test_drops_rows_with_null_order_date(spark):
    df = spark.createDataFrame(
        [
            (1, "Flipkart", "Mumbai", "2026-06-01", "Fashion", 2, 999.0),
            (2, "Swiggy", "Chennai", None, "Food", 1, 300.0),
        ],
        ["order_id", "platform", "city", "order_date", "category", "quantity", "amount"],
    )
    result = clean_sales_df(df)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == 1


SCHEMA = "order_id long, platform string, city string, order_date string, category string, quantity long, amount double"


def test_fills_missing_quantity_with_one(spark):
    df = spark.createDataFrame(
        [(1, "Swiggy", "Pune", "2026-06-01", "Food", None, 250.0)],
        schema=SCHEMA,
    )
    result = clean_sales_df(df)
    assert result.collect()[0]["quantity"] == 1


def test_drops_negative_amounts(spark):
    df = spark.createDataFrame(
        [
            (1, "Zerodha", "Delhi", "2026-06-01", "Brokerage", 1, -50.0),
            (2, "Zerodha", "Delhi", "2026-06-01", "Brokerage", 1, 50.0),
        ],
        ["order_id", "platform", "city", "order_date", "category", "quantity", "amount"],
    )
    result = clean_sales_df(df)
    assert result.count() == 1
    assert result.collect()[0]["amount"] == 50.0


def test_adds_order_month_column(spark):
    df = spark.createDataFrame(
        [(1, "Flipkart", "Mumbai", "2026-06-15", "Electronics", 1, 5000.0)],
        ["order_id", "platform", "city", "order_date", "category", "quantity", "amount"],
    )
    result = clean_sales_df(df)
    assert result.collect()[0]["order_month"] == "2026-06"


def test_deduplicates_by_order_id(spark):
    df = spark.createDataFrame(
        [
            (1, "Swiggy", "Pune", "2026-06-01", "Food", 1, 250.0),
            (1, "Swiggy", "Pune", "2026-06-01", "Food", 1, 250.0),
        ],
        ["order_id", "platform", "city", "order_date", "category", "quantity", "amount"],
    )
    result = clean_sales_df(df)
    assert result.count() == 1

"""Shared SparkSession configuration.

LAKEHOUSE_MODE=local    -> local[*], Parquet output
LAKEHOUSE_MODE=iceberg  -> Iceberg REST catalog on MinIO
"""
import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "customer360") -> SparkSession:
    mode = os.environ.get("LAKEHOUSE_MODE", "local")

    builder = (SparkSession.builder.appName(app_name)
               .config("spark.sql.shuffle.partitions", "8")
               .config("spark.sql.legacy.timeParserPolicy", "CORRECTED"))

    if mode == "iceberg":
        builder = (builder
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.demo.type", "rest")
            .config("spark.sql.catalog.demo.uri", os.environ.get("ICEBERG_REST_URI", "http://rest:8181"))
            .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/")
            .config("spark.sql.catalog.demo.s3.endpoint", os.environ.get("S3_ENDPOINT", "http://minio:9000"))
            .config("spark.sql.defaultCatalog", "demo"))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# Source CSV directory, read by Bronze in both modes.
LOCAL_DATA = os.environ.get("LOCAL_DATA", "./data")

SOURCES = {
    "orders":      "orders_300k_dirty.csv",
    "clickstream": "clickstream_500k_events.csv",
    "crm":         "crm_50000_customers_dirty_v3.csv",
    "tickets":     "support_tickets_30000_dirty.csv",
    "product":     "product_catalog_dirty_30pct.csv",
}

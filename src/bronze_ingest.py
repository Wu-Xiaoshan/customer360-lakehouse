"""Bronze layer: source CSV -> audit columns -> Bronze tables.

No business transformation; all columns stay as strings. Two changes only:
column name normalisation (see lakehouse_io.sanitize_columns) and the audit
columns _source / _ingest_ts / _ingest_run_id.

Row counts and durations are written to metadata.ingestion.
"""
import sys, os, time
from datetime import datetime, timezone

sys.path.append(os.path.dirname(__file__))
from spark_session import get_spark, LOCAL_DATA, SOURCES, RUN_ID
from lakehouse_io import sanitize_columns, write_table, ensure_namespaces, target
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               LongType, DoubleType, TimestampType)

META_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("source", StringType()),
    StructField("rows", LongType()),
    StructField("seconds", DoubleType()),
    StructField("ingested_at", TimestampType()),
])


def ingest_one(spark, name, filename, run_id):
    t0 = time.time()
    df = (spark.read.option("header", True).option("multiLine", True)
          .csv(f"{LOCAL_DATA}/{filename}"))
    df = sanitize_columns(df)
    df = (df.withColumn("_source", F.lit(name))
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_ingest_run_id", F.lit(run_id)))
    n = df.count()
    dest = write_table(df, "bronze", name)
    secs = round(time.time() - t0, 2)
    print(f"[Bronze] {name:12s} rows={n:>6}  {secs:>6}s  -> {dest}")
    return (run_id, name, n, secs, datetime.now(timezone.utc))


def main():
    spark = get_spark("bronze_ingest")
    ensure_namespaces(spark)
    metas = [ingest_one(spark, name, f, RUN_ID) for name, f in SOURCES.items()]

    total = sum(m[2] for m in metas)
    print(f"[Bronze] ingestion complete, {total} rows")

    meta_df = spark.createDataFrame(metas, schema=META_SCHEMA)
    write_table(meta_df, "metadata", "ingestion", mode="append")
    print(f"[Bronze] ingestion metadata -> {target('metadata', 'ingestion')}")

    spark.stop()


if __name__ == "__main__":
    main()

"""Storage abstraction over the two backends.

LAKEHOUSE_MODE=local    -> Parquet at $LOCAL_OUT/{layer}/{name}
LAKEHOUSE_MODE=iceberg  -> Iceberg tables demo.{layer}.{name}
"""
import os
import re
from pyspark.sql import DataFrame, SparkSession

MODE = os.environ.get("LAKEHOUSE_MODE", "local")
CATALOG = os.environ.get("ICEBERG_CATALOG", "demo")
LOCAL_OUT = os.environ.get("LOCAL_OUT", "./warehouse")

LAYERS = ["bronze", "silver", "quarantine", "gold", "metadata"]


def table_name(layer: str, name: str) -> str:
    return f"{CATALOG}.{layer}.{name}"


def _path(layer: str, name: str) -> str:
    return f"{LOCAL_OUT}/{layer}/{name}"


def target(layer: str, name: str) -> str:
    """Resolved location of the table in the current mode."""
    return table_name(layer, name) if MODE == "iceberg" else _path(layer, name)


_DROP = re.compile(r"[(){}]")
_TO_US = re.compile(r"[ ,;\n\t=]+")


def _clean_name(c: str) -> str:
    return _TO_US.sub("_", _DROP.sub("", c)).strip("_").lower()


def sanitize_columns(df: DataFrame) -> DataFrame:
    """Normalise CSV headers to snake_case, e.g. `device_id(s)` -> `device_ids`."""
    out = df
    for c in df.columns:
        new = _clean_name(c)
        if new != c:
            out = out.withColumnRenamed(c, new)
    return out


def ensure_namespaces(spark: SparkSession) -> None:
    """Create the layer namespaces; required before writeTo in iceberg mode."""
    if MODE != "iceberg":
        return
    for layer in LAYERS:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{layer}")


def write_table(df: DataFrame, layer: str, name: str, mode: str = "overwrite") -> str:
    """overwrite by default; append is used only for the ingestion metadata log."""
    if MODE == "iceberg":
        t = table_name(layer, name)
        if mode == "append" and df.sparkSession.catalog.tableExists(t):
            df.writeTo(t).append()
        else:
            df.writeTo(t).createOrReplace()
        return t
    p = _path(layer, name)
    df.write.mode("append" if mode == "append" else "overwrite").parquet(p)
    return p


def read_table(spark: SparkSession, layer: str, name: str) -> DataFrame:
    if MODE == "iceberg":
        return spark.table(table_name(layer, name))
    return spark.read.parquet(_path(layer, name))

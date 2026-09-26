"""Rule-based data quality framework.

A rule is a (name, condition) pair. Rules passed as `rules` reject a row into
quarantine; rules passed as `warnings` keep the row and record the reason in `_warn`.
"""
from pyspark.sql import functions as F, DataFrame, Window


# ---------- reusable rules ----------
def rule_not_null(col):
    return (f"missing_{col}",
            F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == ""))


def rule_positive(col):
    return (f"nonpositive_{col}", F.col(col).isNotNull() & (F.col(col) <= 0))


def rule_not_nan(col):
    """NaN survives try_cast and is neither null nor <= 0, so it needs its own rule."""
    return (f"nan_{col}", F.isnan(F.col(col)))


def rule_bad_after_parse(col):
    """Null after a try_* cast, meaning the original value was malformed."""
    return (f"bad_{col}", F.col(col).isNull())


def rule_regex_invalid(col, pattern, name=None):
    return (name or f"invalid_{col}", ~F.col(col).rlike(pattern))


def rule_matches(col, pattern, name):
    return (name, F.col(col).isNotNull() & F.lower(F.col(col)).rlike(pattern))


def rule_after(col, date, name=None):
    """Dated after the snapshot reference date."""
    return (name or f"after_as_of_{col}", F.to_date(F.col(col)) > F.to_date(F.lit(date)))


# ---------- core ----------
def _reasons(rules):
    cols = [F.when(cond, F.lit(name)) for name, cond in rules]
    return F.array_compact(F.array(*cols)) if cols else F.array().cast("array<string>")


def apply_dq(df: DataFrame, rules: list, warnings: list = ()) -> DataFrame:
    """Attach `_err` (reject reasons) and `_warn` (warning reasons) arrays."""
    return df.withColumn("_err", _reasons(rules)).withColumn("_warn", _reasons(list(warnings)))


def split_valid_quarantine(df: DataFrame):
    """Empty `_err` -> valid, which keeps its `_warn`; non-empty -> quarantine."""
    valid = df.filter(F.size("_err") == 0).drop("_err")
    quar = df.filter(F.size("_err") > 0)
    return valid, quar


def dedup(df: DataFrame, key: str, order_by: list):
    """Keep the first row per `key` under `order_by`; return (kept, duplicates).

    Duplicates carry `_err = [duplicate_<key>]` so they can be added to the quarantine.
    """
    w = Window.partitionBy(key).orderBy(*order_by)
    ranked = df.withColumn("_rn", F.row_number().over(w))
    kept = ranked.filter(F.col("_rn") == 1).drop("_rn")
    dups = (ranked.filter(F.col("_rn") > 1).drop("_rn")
            .withColumn("_err", F.array(F.lit(f"duplicate_{key}"))))
    return kept, dups


SUMMARY_SCHEMA = ("run_id string, source string, severity string, reason string, "
                  "rows bigint, source_rows bigint")


def dq_summary(spark, run_id: str, source: str, valid: DataFrame, quar: DataFrame) -> DataFrame:
    """Print reject and warning counts and return them as rows for metadata.dq_summary."""
    nv, nq = valid.count(), quar.count()
    total = nv + nq
    if total == 0:
        print(f"[DQ][{source}] total=0 — the Bronze table is empty; check the previous layer")
        return spark.createDataFrame([], SUMMARY_SCHEMA)
    print(f"[DQ][{source}] total={total} valid={nv} quarantined={nq} "
          f"({round(nq / total * 100, 1)}% rejected)")

    def counts(df, col, severity):
        return [(run_id, source, severity, r["reason"], r["count"], total)
                for r in df.select(F.explode(col).alias("reason")).groupBy("reason").count().collect()]

    rows = counts(quar, "_err", "reject") + counts(valid, "_warn", "warn")
    summary = spark.createDataFrame(rows, SUMMARY_SCHEMA)
    summary.orderBy("severity", F.desc("rows")).select("severity", "reason", "rows").show(truncate=False)
    return summary

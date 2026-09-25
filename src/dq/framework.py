"""Rule-based data quality framework.

A rule is a (name, condition) pair; each source passes its own list.
"""
from pyspark.sql import functions as F, DataFrame


# ---------- reusable rules ----------
def rule_not_null(col):
    return (f"missing_{col}",
            F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == ""))


def rule_positive(col):
    return (f"nonpositive_{col}", F.col(col).isNotNull() & (F.col(col) <= 0))


def rule_bad_after_parse(col):
    """Null after a try_* cast, meaning the original value was malformed."""
    return (f"bad_{col}", F.col(col).isNull())


def rule_regex_invalid(col, pattern, name=None):
    return (name or f"invalid_{col}", ~F.col(col).rlike(pattern))


# ---------- core ----------
def apply_dq(df: DataFrame, rules: list) -> DataFrame:
    """Attach `_err`, an array of the rule names each row violates."""
    err_cols = [F.when(cond, F.lit(name)) for name, cond in rules]
    return df.withColumn("_err", F.array_compact(F.array(*err_cols)))


def split_valid_quarantine(df: DataFrame):
    """Empty `_err` -> valid; non-empty -> quarantine."""
    valid = df.filter(F.size("_err") == 0).drop("_err")
    quar = df.filter(F.size("_err") > 0)
    return valid, quar


def dq_summary(source_name: str, valid_df: DataFrame, quar_df: DataFrame):
    """Print and return the rejection counts by reason."""
    nv, nq = valid_df.count(), quar_df.count()
    total = nv + nq
    if total == 0:
        print(f"[DQ][{source_name}] total=0 — the Bronze table is empty; check the previous layer")
        return quar_df.select(F.lit(None).alias("reason"), F.lit(0).alias("count")).limit(0)
    print(f"[DQ][{source_name}] total={total} valid={nv} quarantined={nq} "
          f"({round(nq/total*100,1)}% rejected)")
    reasons = (quar_df.select(F.explode("_err").alias("reason"))
               .groupBy("reason").count().orderBy(F.desc("count")))
    reasons.show(truncate=False)
    return reasons

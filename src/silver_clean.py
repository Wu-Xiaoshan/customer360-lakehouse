"""Silver layer: Bronze tables -> standardisation -> DQ rules -> valid / quarantine.

Rules reject a row into quarantine.{name}; warnings keep it and record the reason in
its `_warn` column. Every source is deduplicated on its primary key, and the CRM keeps
one golden record per customer. Reason counts go to metadata.dq_summary and orphan
counts per relationship to metadata.integrity.
"""
import sys, os

sys.path.append(os.path.dirname(__file__))
from spark_session import get_spark, AS_OF_DATE, RUN_ID
from lakehouse_io import read_table, write_table, ensure_namespaces, target
from dq.framework import (rule_not_null, rule_positive, rule_not_nan, rule_bad_after_parse,
                          rule_regex_invalid, rule_matches, rule_after,
                          apply_dq, split_valid_quarantine, dedup, dq_summary)
from pyspark.sql import functions as F

NAME_NOISE = r"[^A-Za-z '\-]"
DOUBLED_LETTERS = r"(.)\1.*(.)\2.*(.)\3"
CRM_PROFILE = ["first_name", "last_name", "email", "phone_number", "gender", "dob",
               "signup_date", "address", "city", "state", "country", "source", "device_ids"]
CATEGORIES = ["clothing", "kitchen", "beauty", "sports",
              "home", "automotive", "toys", "electronics"]


def _read(spark, name):
    return read_table(spark, "bronze", name)


def _stable_hash(df):
    """Hash of the business columns; the last tie-break, so reruns keep the same row."""
    return F.xxhash64(*[F.col(c) for c in df.columns if not c.startswith("_")])


def _finish(spark, name, df, key, rules, warnings=(), order_by=None, repair=None, drop=()):
    """Validate, repair, deduplicate on `key`, write, and return (silver table, summary)."""
    valid, quar = split_valid_quarantine(apply_dq(df, rules, warnings))
    if repair:
        valid = repair(valid)
    valid, dups = dedup(valid, key, order_by or [F.size("_warn"), _stable_hash(df)])
    quar = quar.unionByName(dups, allowMissingColumns=True)
    valid, quar = valid.drop(*drop), quar.drop(*drop)
    summary = dq_summary(spark, RUN_ID, name, valid, quar)
    write_table(valid, "silver", name)
    write_table(quar, "quarantine", name)
    print(f"[Silver] {name:12s} -> {target('silver', name)} | {target('quarantine', name)}")
    return read_table(spark, "silver", name), summary


def _signup(crm):
    return crm.select("customer_id", F.col("signup_date").alias("_signup_date"))


def clean_crm(spark):
    df = (_read(spark, "crm")
        .withColumn("first_name", F.initcap(F.trim(F.regexp_replace("first_name", NAME_NOISE, ""))))
        .withColumn("last_name", F.initcap(F.trim(F.regexp_replace("last_name", NAME_NOISE, ""))))
        .withColumn("gender", F.upper(F.trim("gender")))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("phone_digits", F.regexp_replace("phone_number", r"\D", ""))
        .withColumn("dob", F.expr("to_date(try_to_timestamp(dob,'yyyy-MM-dd'))"))
        .withColumn("signup_date", F.expr("to_date(try_to_timestamp(signup_date,'yyyy-MM-dd'))")))
    rules = [rule_not_null("customer_id"),
             rule_regex_invalid("email", r"^\S+@\S+\.\S+$"),
             rule_after("signup_date", AS_OF_DATE)]
    warnings = [rule_matches("first_name", DOUBLED_LETTERS, "corrupted_first_name"),
                rule_matches("last_name", DOUBLED_LETTERS, "corrupted_last_name")]

    # Golden record: fewest warnings, then the most populated profile, then a stable hash.
    populated = sum(F.when(F.col(c).isNotNull() & (F.trim(F.col(c).cast("string")) != ""), 1)
                     .otherwise(0) for c in CRM_PROFILE)
    crm, summary = _finish(spark, "crm", df, "customer_id", rules, warnings,
                           order_by=[F.size("_warn"), populated.desc(), _stable_hash(df)])

    dev = (crm.select("customer_id", F.explode(F.split(F.col("device_ids"), ";")).alias("device_id"))
           .withColumn("device_id", F.trim("device_id")).filter(F.col("device_id") != "")
           .dropDuplicates(["customer_id", "device_id"]))
    write_table(dev, "silver", "customer_device")
    print(f"[Silver] customer_device -> {target('silver', 'customer_device')}")
    return crm, summary


def _canonical_category(col):
    """Map truncated (clo), suffixed (clothing-) and leetspeak (3l3ctronics) values
    onto CATEGORIES. Anything unrecognisable becomes null and is quarantined."""
    c = F.regexp_replace(F.lower(F.trim(col)), "[^a-z3]", "")
    c = F.regexp_replace(c, "3", "e")
    out = None
    for name in CATEGORIES:
        cond = (F.length(c) >= 3) & (F.lit(name).startswith(c) | c.startswith(name))
        out = F.when(cond, F.lit(name)) if out is None else out.when(cond, F.lit(name))
    return out


def clean_product(spark):
    df = (_read(spark, "product")
        .withColumn("price", F.expr("try_cast(regexp_replace(price,'[\\s,]','') as double)"))
        .withColumn("category", _canonical_category(F.col("category")))
        .withColumn("product_name", F.initcap(F.trim("product_name"))))
    rules = [rule_not_null("product_id"), rule_positive("price"),
             rule_not_nan("price"), rule_not_null("category")]
    return _finish(spark, "product", df, "product_id", rules)


def clean_orders(spark, crm):
    df = (_read(spark, "orders")
        .withColumn("order_amount", F.expr("try_cast(regexp_replace(order_amount,'[\\s,]','') as double)"))
        .withColumn("order_date", F.expr("to_date(try_to_timestamp(order_date,'yyyy-MM-dd'))"))
        .withColumn("status",
            F.when(F.lower(F.trim("status")).rlike("^suc"),"success")
             .when(F.lower(F.trim("status")).rlike("^ref"),"refunded")
             .when(F.lower(F.trim("status")).rlike("^fail"),"failed").otherwise("unknown"))
        .withColumn("payment_method",
            F.when(F.lower(F.regexp_replace("payment_method",r"[\s@]","")).rlike("card|crad"),"card")
             .when(F.lower(F.trim("payment_method")).rlike("wall"),"wallet")
             .when(F.lower(F.trim("payment_method")).rlike("upi"),"upi")
             .when(F.lower(F.trim("payment_method")).rlike("cash"),"cash").otherwise("unknown"))
        .withColumn("quantity", F.expr("try_cast(quantity as int)"))
        .join(_signup(crm), "customer_id", "left"))
    rules = [rule_not_null("order_id"), rule_not_null("customer_id"), rule_not_null("product_id"),
             rule_bad_after_parse("order_date"), rule_after("order_date", AS_OF_DATE),
             rule_bad_after_parse("order_amount"), rule_not_nan("order_amount"),
             rule_positive("order_amount")]
    warnings = [rule_not_null("quantity"), rule_positive("quantity"),
                ("order_before_signup", F.col("order_date") < F.col("_signup_date")),
                ("unknown_payment_method", F.col("payment_method") == "unknown")]
    return _finish(spark, "orders", df, "order_id", rules, warnings,
                   repair=lambda d: d.withColumn("quantity", F.when(F.col("quantity") > 0, F.col("quantity"))),
                   drop=["_signup_date"])


def clean_tickets(spark, crm):
    df = (_read(spark, "tickets")
        .withColumn("created", F.expr("try_to_timestamp(ticket_created)"))
        .withColumn("resolved", F.expr("try_to_timestamp(ticket_resolved)"))
        .withColumn("resolution_hours",
            F.round((F.unix_timestamp("resolved") - F.unix_timestamp("created")) / 3600, 2))
        .withColumn("sentiment",
            F.when(F.lower(F.trim("sentiment")).rlike("^pos"),"positive")
             .when(F.lower(F.trim("sentiment")).rlike("^neg"),"negative")
             .when(F.lower(F.trim("sentiment")).rlike("^neu|^n3u"),"neutral").otherwise("unknown"))
        .withColumn("issue_type",
            F.when(F.lower(F.trim("issue_type")).rlike("pro"),"product")
             .when(F.lower(F.trim("issue_type")).rlike("ref|fund"),"refund")
             .when(F.lower(F.trim("issue_type")).rlike("del"),"delay")
             .when(F.lower(F.trim("issue_type")).rlike("pay|yap"),"payment").otherwise("other"))
        .withColumn("agent", F.initcap(F.trim("support_agent")))
        .join(_signup(crm), "customer_id", "left"))
    rules = [rule_not_null("ticket_id"), rule_not_null("customer_id"),
             rule_bad_after_parse("created"), rule_after("created", AS_OF_DATE),
             ("resolved_before_created",
              F.col("resolved").isNotNull() & F.col("created").isNotNull() & (F.col("resolved") < F.col("created")))]
    warnings = [("ticket_before_signup", F.to_date("created") < F.col("_signup_date")),
                rule_after("resolved", AS_OF_DATE, "resolved_after_as_of"),
                ("resolution_time_mismatch",
                 F.abs(F.expr("try_cast(resolution_time_hours as double)") - F.col("resolution_hours")) > 1)]
    return _finish(spark, "tickets", df, "ticket_id", rules, warnings, drop=["_signup_date"])


def clean_clickstream(spark):
    df = (_read(spark, "clickstream")
        .withColumn("event_time", F.expr("try_to_timestamp(timestamp)"))
        .withColumn("event_type",
            F.when(F.lower(F.trim("event_type")).rlike("page|view"),"page_view")
             .when(F.lower(F.trim("event_type")).rlike("search"),"search")
             .when(F.lower(F.trim("event_type")).rlike("cart|add"),"add_to_cart")
             .when(F.lower(F.trim("event_type")).rlike("log"),"login").otherwise("other")))
    rules = [rule_not_null("event_id"), rule_not_null("customer_id"),
             rule_bad_after_parse("event_time"), rule_after("event_time", AS_OF_DATE)]
    return _finish(spark, "clickstream", df, "event_id", rules)


INTEGRITY_SCHEMA = ("run_id string, relationship string, child_rows bigint, "
                    "orphans_vs_bronze bigint, orphans_vs_silver bigint")


def _orphans(child, col, parent, parent_col):
    keys = parent.select(F.col(parent_col).alias(col)).distinct()
    return (child.filter(F.col(col).isNotNull() & (F.trim(F.col(col)) != ""))
                 .join(keys, col, "left_anti").count())


def _devices(crm):
    return (crm.select(F.explode(F.split(F.col("device_ids"), ";")).alias("device_id"))
               .select(F.trim("device_id").alias("device_id")))


def integrity(spark, silver):
    """Orphans per relationship, measured against the Bronze and the Silver parent."""
    b_crm, b_product = _read(spark, "crm"), _read(spark, "product")
    s_devices = read_table(spark, "silver", "customer_device")
    checks = [
        ("orders.product_id -> product", "orders", "product_id", b_product, silver["product"], "product_id"),
        ("orders.customer_id -> crm", "orders", "customer_id", b_crm, silver["crm"], "customer_id"),
        ("tickets.customer_id -> crm", "tickets", "customer_id", b_crm, silver["crm"], "customer_id"),
        ("clickstream.customer_id -> crm", "clickstream", "customer_id", b_crm, silver["crm"], "customer_id"),
        ("clickstream.device_id -> crm.device_ids", "clickstream", "device_id",
         _devices(b_crm), s_devices, "device_id"),
    ]
    rows = [(RUN_ID, rel, silver[child].count(),
             _orphans(silver[child], col, b_parent, pcol),
             _orphans(silver[child], col, s_parent, pcol))
            for rel, child, col, b_parent, s_parent, pcol in checks]
    df = spark.createDataFrame(rows, INTEGRITY_SCHEMA)
    df.drop("run_id").show(truncate=False)
    return df


def main():
    spark = get_spark("silver_clean")
    ensure_namespaces(spark)
    crm, s_crm = clean_crm(spark)
    product, s_product = clean_product(spark)
    orders, s_orders = clean_orders(spark, crm)
    tickets, s_tickets = clean_tickets(spark, crm)
    click, s_click = clean_clickstream(spark)

    summary = (s_crm.unionByName(s_product).unionByName(s_orders)
               .unionByName(s_tickets).unionByName(s_click))
    write_table(summary, "metadata", "dq_summary", mode="append")
    print(f"[Silver] DQ summary -> {target('metadata', 'dq_summary')}")

    silver = {"crm": crm, "product": product, "orders": orders,
              "tickets": tickets, "clickstream": click}
    write_table(integrity(spark, silver), "metadata", "integrity", mode="append")
    print(f"[Silver] integrity -> {target('metadata', 'integrity')}")

    print("[Silver] cleaning complete")
    spark.stop()


if __name__ == "__main__":
    main()

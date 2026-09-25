"""Silver layer: Bronze tables -> standardisation -> DQ rules -> valid / quarantine.

Rejected rows are written to quarantine.{name} with their `_err` reasons.
"""
import sys, os

sys.path.append(os.path.dirname(__file__))
from spark_session import get_spark
from lakehouse_io import read_table, write_table, ensure_namespaces, target
from dq.framework import (rule_not_null, rule_positive, rule_bad_after_parse,
                          rule_regex_invalid, apply_dq, split_valid_quarantine, dq_summary)
from pyspark.sql import functions as F


def _read(spark, name):
    return read_table(spark, "bronze", name)


def _write(valid, quar, name):
    write_table(valid, "silver", name)
    write_table(quar, "quarantine", name)
    print(f"[Silver] {name:12s} -> {target('silver', name)} | {target('quarantine', name)}")


def clean_orders(spark):
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
        .withColumn("quantity", F.expr("try_cast(quantity as int)")))
    rules = [rule_not_null("customer_id"), rule_not_null("product_id"),
             rule_bad_after_parse("order_date"), rule_bad_after_parse("order_amount"),
             rule_positive("order_amount")]
    valid, quar = split_valid_quarantine(apply_dq(df, rules))
    dq_summary("orders", valid, quar); _write(valid, quar, "orders")
    return valid


def clean_clickstream(spark):
    df = (_read(spark, "clickstream")
        .withColumn("event_time", F.expr("try_to_timestamp(timestamp)"))
        .withColumn("event_type",
            F.when(F.lower(F.trim("event_type")).rlike("page|view"),"page_view")
             .when(F.lower(F.trim("event_type")).rlike("search"),"search")
             .when(F.lower(F.trim("event_type")).rlike("cart|add"),"add_to_cart")
             .when(F.lower(F.trim("event_type")).rlike("log"),"login").otherwise("other")))
    rules = [rule_not_null("customer_id"), rule_bad_after_parse("event_time")]
    valid, quar = split_valid_quarantine(apply_dq(df, rules))
    dq_summary("clickstream", valid, quar); _write(valid, quar, "clickstream")
    return valid


def clean_tickets(spark):
    df = (_read(spark, "tickets")
        .withColumn("created", F.expr("try_to_timestamp(ticket_created)"))
        .withColumn("resolved", F.expr("try_to_timestamp(ticket_resolved)"))
        .withColumn("sentiment",
            F.when(F.lower(F.trim("sentiment")).rlike("^pos"),"positive")
             .when(F.lower(F.trim("sentiment")).rlike("^neg"),"negative")
             .when(F.lower(F.trim("sentiment")).rlike("^neu|^n3u"),"neutral").otherwise("unknown"))
        .withColumn("issue_type",
            F.when(F.lower(F.trim("issue_type")).rlike("pro"),"product")
             .when(F.lower(F.trim("issue_type")).rlike("ref|fund"),"refund")
             .when(F.lower(F.trim("issue_type")).rlike("del"),"delay")
             .when(F.lower(F.trim("issue_type")).rlike("pay|yap"),"payment").otherwise("other"))
        .withColumn("agent", F.initcap(F.trim("support_agent"))))
    rules = [rule_not_null("customer_id"), rule_bad_after_parse("created"),
             ("resolved_before_created",
              F.col("resolved").isNotNull() & F.col("created").isNotNull() & (F.col("resolved") < F.col("created")))]
    valid, quar = split_valid_quarantine(apply_dq(df, rules))
    dq_summary("tickets", valid, quar); _write(valid, quar, "tickets")
    return valid


def clean_crm(spark):
    df = (_read(spark, "crm")
        .withColumn("first_name", F.initcap(F.trim("first_name")))
        .withColumn("last_name", F.initcap(F.trim("last_name")))
        .withColumn("gender", F.upper(F.trim("gender")))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("phone_digits", F.regexp_replace("phone_number", r"\D", ""))
        .withColumn("dob", F.expr("to_date(try_to_timestamp(dob,'yyyy-MM-dd'))"))
        .withColumn("signup_date", F.expr("to_date(try_to_timestamp(signup_date,'yyyy-MM-dd'))")))
    rules = [rule_not_null("customer_id"),
             rule_regex_invalid("email", r"^\S+@\S+\.\S+$")]
    valid, quar = split_valid_quarantine(apply_dq(df, rules))
    dq_summary("crm", valid, quar); _write(valid, quar, "crm")
    # Multi-value device_ids exploded into a link table.
    dev = (valid.select("customer_id",
             F.explode(F.split(F.col("device_ids"), ";")).alias("device_id"))
           .withColumn("device_id", F.trim("device_id")).filter(F.col("device_id") != ""))
    write_table(dev, "silver", "customer_device")
    print(f"[Silver] customer_device -> {target('silver', 'customer_device')}")
    return valid


def clean_product(spark):
    df = (_read(spark, "product")
        .withColumn("price", F.expr("try_cast(regexp_replace(price,'[\\s,]','') as double)"))
        .withColumn("category", F.lower(F.trim(F.regexp_replace("category","3","e"))))
        .withColumn("product_name", F.initcap(F.trim("product_name"))))
    rules = [rule_not_null("product_id"), rule_positive("price"), rule_not_null("category")]
    valid, quar = split_valid_quarantine(apply_dq(df, rules))
    dq_summary("product", valid, quar); _write(valid, quar, "product")
    return valid


def main():
    spark = get_spark("silver_clean")
    ensure_namespaces(spark)
    clean_orders(spark); clean_clickstream(spark); clean_tickets(spark)
    clean_crm(spark); clean_product(spark)
    print("[Silver] cleaning complete")
    spark.stop()


if __name__ == "__main__":
    main()

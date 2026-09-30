"""Gold layer: Silver tables -> seven analytics tables.

  customer_360          one row per customer: profile, purchases, support, recent behaviour
  customer_rfm          RFM segmentation
  customer_risk         churn risk score
  campaign_priority     retention priority
  category_penetration  buyers per category; orders whose product was quarantined count as unknown
  behavior_funnel       nested conversion funnel over the behaviour window
  cart_abandoners       carted in the behaviour window without an order in it

Time is measured from AS_OF_DATE; behaviour metrics cover the BEHAVIOUR_WINDOW_DAYS before it.
"""
import sys, os

sys.path.append(os.path.dirname(__file__))
from spark_session import get_spark, AS_OF_DATE, BEHAVIOUR_WINDOW_DAYS as W
from lakehouse_io import read_table, write_table, ensure_namespaces, target
from pyspark.sql import functions as F, Window


def _as_of():
    return F.to_date(F.lit(AS_OF_DATE))


def _quintile(order):
    """Score 1 to 5 from percent_rank; tied values always share a score."""
    return F.least(F.lit(5), F.floor(F.percent_rank().over(Window.orderBy(order)) * 5) + 1).cast("int")


def build_rfm(succ):
    rfm = succ.groupBy("customer_id").agg(
        F.datediff(_as_of(), F.max("order_date")).alias("recency_days"),
        F.count("*").alias("frequency"),
        F.round(F.sum("order_amount"), 2).alias("monetary"))
    # R and Ms are quintiles; Fs is the order count capped at 5.
    rfm = (rfm
        .withColumn("R", _quintile(F.col("recency_days").desc()))
        .withColumn("Fs", F.least(F.col("frequency"), F.lit(5)).cast("int"))
        .withColumn("Ms", _quintile(F.col("monetary").asc())))
    rfm = rfm.withColumn("segment",
        F.when((F.col("R")>=4)&(F.col("Fs")>=4),"Champions")
         .when((F.col("R")>=3)&(F.col("Fs")>=3),"Loyal")
         .when((F.col("R")<=2)&(F.col("Fs")>=3),"At Risk")
         .when((F.col("R")<=2)&(F.col("Fs")<=2),"Hibernating").otherwise("Others"))
    return rfm


def build_risk(rfm, orders, tickets):
    rf = (orders.filter(F.col("status").isin("refunded","failed"))
          .groupBy("customer_id").agg(F.count("*").alias("bad_orders")))
    neg = (tickets.filter(F.col("sentiment")=="negative")
           .groupBy("customer_id").agg(F.count("*").alias("neg_tickets")))
    risk = (rfm.select("customer_id","recency_days","segment")
        .join(rf,"customer_id","left").join(neg,"customer_id","left")
        .fillna(0,["bad_orders","neg_tickets"])
        .withColumn("risk_score",
            (F.when(F.col("recency_days")>90,2).when(F.col("recency_days")>30,1).otherwise(0))
            + F.when(F.col("bad_orders")>=2,2).when(F.col("bad_orders")==1,1).otherwise(0)
            + F.when(F.col("neg_tickets")>=2,2).when(F.col("neg_tickets")==1,1).otherwise(0))
        .withColumn("risk_level",
            F.when(F.col("risk_score")>=4,"High").when(F.col("risk_score")>=2,"Medium")
             .when(F.col("risk_score")==1,"Low").otherwise("Very Low")))
    return risk


def build_priority(rfm, risk):
    return (rfm.select("customer_id","segment","Ms")
        .join(risk.select("customer_id","risk_score"), "customer_id")
        .withColumn("priority",
            F.when((F.col("Ms")>=4) & (F.col("risk_score")>=2), "HIGH")
             .when((F.col("Ms")>=3) | (F.col("risk_score")>=2), "MEDIUM")
             .otherwise("LOW")))


def build_penetration(succ, product):
    buyers = succ.select("customer_id").distinct().count()
    return (succ.join(product.select("product_id", "category"), "product_id", "left")
            .withColumn("category", F.coalesce(F.col("category"), F.lit("unknown")))
            .groupBy("category").agg(F.countDistinct("customer_id").alias("customers"))
            .withColumn("penetration_pct", F.round(F.col("customers") / buyers * 100, 1))
            .orderBy(F.desc("customers")))


FUNNEL_STEPS = ["page_view", "search", "add_to_cart"]

FUNNEL_SCHEMA = "step_no int, step string, customers bigint"


def build_funnel(spark, click, succ):
    """Nested funnel: each step counts customers who also did every earlier step.

    Counting distinct customers per event type independently does not decrease from
    step to step, because the event types are not nested. Intersecting the customer
    sets does, and the drop between steps is the conversion loss.
    """
    stages, current = [], None
    for i, event in enumerate(FUNNEL_STEPS, start=1):
        doers = click.filter(F.col("event_type") == event).select("customer_id").distinct()
        current = doers if current is None else current.join(doers, "customer_id")
        stages.append((i, event, current.count()))

    buyers = succ.select("customer_id").distinct()
    stages.append((len(FUNNEL_STEPS) + 1, "purchase",
                   current.join(buyers, "customer_id").count()))
    return spark.createDataFrame(stages, schema=FUNNEL_SCHEMA)


def build_cart_abandoners(click, succ):
    """Customers with add_to_cart events and no successful order."""
    carted = (click.filter(F.col("event_type") == "add_to_cart")
              .groupBy("customer_id").agg(F.count("*").alias("cart_events")))
    buyers = succ.select("customer_id").distinct()
    return carted.join(buyers, "customer_id", "left_anti").orderBy(F.desc("cart_events"))


def build_customer_360(crm, rfm, risk, priority, orders, succ, product, tickets, click_w):
    """One row per customer: masked profile, purchases, support history and behaviour in
    the window. No email, phone or address."""
    as_of = _as_of()
    profile = (crm
        .withColumn("full_name_masked",
            F.concat(F.substring("first_name", 1, 1), F.lit("*** "),
                     F.substring("last_name", 1, 1), F.lit("**")))
        .withColumn("tenure_days", F.datediff(as_of, "signup_date"))
        .select("customer_id", "full_name_masked", "state", "country", "gender", "source",
                "tenure_days"))

    bad = (orders.filter(F.col("status").isin("refunded", "failed"))
           .groupBy("customer_id").agg(F.count("*").alias("bad_orders")))

    per_category = (succ.join(product.select("product_id", "category"), "product_id")
                    .groupBy("customer_id", "category").count())
    favourite = (per_category
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("customer_id").orderBy(F.desc("count"), "category")))
        .filter(F.col("_rn") == 1)
        .select("customer_id", F.col("category").alias("favourite_category")))

    support = tickets.groupBy("customer_id").agg(
        F.count("*").alias("tickets_total"),
        F.sum(F.when(F.col("resolved").isNull() | (F.to_date("resolved") > as_of), 1)
               .otherwise(0)).alias("open_tickets"),
        F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("negative_tickets"),
        F.max(F.struct("created", "ticket_id", "issue_type"))["issue_type"].alias("last_issue_type"))

    behaviour = click_w.groupBy("customer_id").agg(
        F.count("*").alias(f"events_{W}d"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(f"cart_events_{W}d"),
        F.max("event_time").alias("last_event_time"))

    counts = ["bad_orders", "tickets_total", "open_tickets", "negative_tickets",
              f"events_{W}d", f"cart_events_{W}d"]
    return (profile
        .join(rfm.select("customer_id", "recency_days", "frequency", "monetary", "segment"),
              "customer_id", "left")
        .join(bad, "customer_id", "left")
        .join(favourite, "customer_id", "left")
        .join(support, "customer_id", "left")
        .join(behaviour, "customer_id", "left")
        .join(risk.select("customer_id", "risk_score", "risk_level"), "customer_id", "left")
        .join(priority.select("customer_id", "priority"), "customer_id", "left")
        .fillna(0, counts)
        .fillna({"segment": "No Purchase", "risk_level": "Unknown", "priority": "N/A"}))


def main():
    spark = get_spark("gold_metrics")
    ensure_namespaces(spark)

    orders  = read_table(spark, "silver", "orders")
    tickets = read_table(spark, "silver", "tickets")
    product = read_table(spark, "silver", "product")
    crm     = read_table(spark, "silver", "crm")
    click   = read_table(spark, "silver", "clickstream")

    succ = orders.filter((F.col("status")=="success") & F.col("order_date").isNotNull()
                         & (F.col("order_amount")>0) & (F.col("order_date") <= _as_of()))
    start = F.date_sub(_as_of(), W)
    succ_w = succ.filter(F.col("order_date") >= start)
    click_w = click.filter(F.to_date("event_time") >= start)
    print(f"[Gold] as_of={AS_OF_DATE}  behaviour window={W} days")

    rfm = build_rfm(succ)
    write_table(rfm, "gold", "customer_rfm")
    print(f"== RFM segments == -> {target('gold','customer_rfm')}")
    rfm.groupBy("segment").count().orderBy(F.desc("count")).show()

    risk = build_risk(rfm, orders, tickets)
    write_table(risk, "gold", "customer_risk")
    print(f"== Churn risk == -> {target('gold','customer_risk')}")
    risk.groupBy("risk_level").count().orderBy(F.desc("count")).show()

    priority = build_priority(rfm, risk)
    write_table(priority, "gold", "campaign_priority")
    print(f"== Campaign priority == -> {target('gold','campaign_priority')}")
    priority.groupBy("priority").count().orderBy(F.desc("count")).show()

    pen = build_penetration(succ, product)
    write_table(pen, "gold", "category_penetration")
    print(f"== Category penetration == -> {target('gold','category_penetration')}")
    pen.show()

    funnel = build_funnel(spark, click_w, succ_w)
    write_table(funnel, "gold", "behavior_funnel")
    print(f"== Behaviour funnel ({W} days) == -> {target('gold','behavior_funnel')}")
    funnel.show()

    aband = build_cart_abandoners(click_w, succ_w)
    write_table(aband, "gold", "cart_abandoners")
    print(f"== Cart abandoners ({W} days) == -> {target('gold','cart_abandoners')}  {aband.count()} customers")

    c360 = build_customer_360(crm, rfm, risk, priority, orders, succ, product, tickets, click_w)
    write_table(c360, "gold", "customer_360")
    c360 = read_table(spark, "gold", "customer_360")
    print(f"== Customer 360 == -> {target('gold','customer_360')}  {c360.count()} rows, "
          f"{c360.select('customer_id').distinct().count()} customers")
    c360.show(2, truncate=False, vertical=True)

    print("[Gold] metrics complete")
    spark.stop()


if __name__ == "__main__":
    main()

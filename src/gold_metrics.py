"""Gold layer: Silver tables -> seven analytics tables.

  customer_rfm          RFM segmentation
  customer_risk         churn risk score
  campaign_priority     retention priority
  category_penetration  distinct customers per category
  behavior_funnel       conversion funnel
  cart_abandoners       added to cart, never purchased
  customer_360          one row per customer
"""
import sys, os

sys.path.append(os.path.dirname(__file__))
from spark_session import get_spark
from lakehouse_io import read_table, write_table, ensure_namespaces, target
from pyspark.sql import functions as F, Window


def build_rfm(succ):
    max_date = succ.agg(F.max("order_date")).collect()[0][0]
    rfm = succ.groupBy("customer_id").agg(
        F.datediff(F.lit(max_date), F.max("order_date")).alias("recency_days"),
        F.count("*").alias("frequency"),
        F.round(F.sum("order_amount"), 2).alias("monetary"))
    rfm = (rfm
        .withColumn("R", F.ntile(5).over(Window.orderBy(F.col("recency_days").desc())))
        .withColumn("Fs", F.ntile(5).over(Window.orderBy(F.col("frequency").asc())))
        .withColumn("Ms", F.ntile(5).over(Window.orderBy(F.col("monetary").asc()))))
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


def build_penetration(succ, product, total_customers):
    return (succ.join(product, "product_id").groupBy("category")
            .agg(F.countDistinct("customer_id").alias("customers"))
            .withColumn("penetration_pct", F.round(F.col("customers")/total_customers*100, 1))
            .orderBy(F.desc("customers")))


FUNNEL_STEPS = ["login", "search", "page_view", "add_to_cart"]


def build_funnel(click):
    """Distinct customers per event type; `step_no` carries the funnel order."""
    order_expr = F.create_map(*[x for i, s in enumerate(FUNNEL_STEPS)
                                for x in (F.lit(s), F.lit(i + 1))])
    return (click.groupBy("event_type")
            .agg(F.countDistinct("customer_id").alias("customers"))
            .withColumn("step_no", order_expr[F.col("event_type")])
            .filter(F.col("step_no").isNotNull())
            .orderBy("step_no"))


def build_cart_abandoners(click, succ):
    """Customers with add_to_cart events and no successful order."""
    carted = (click.filter(F.col("event_type") == "add_to_cart")
              .groupBy("customer_id").agg(F.count("*").alias("cart_events")))
    buyers = succ.select("customer_id").distinct()
    return carted.join(buyers, "customer_id", "left_anti").orderBy(F.desc("cart_events"))


def build_customer_360(crm, rfm, risk, priority):
    """Masked name, geography, RFM, risk and priority. No email, phone or address."""
    masked = (crm
        .withColumn("full_name_masked",
            F.concat(F.substring("first_name", 1, 1), F.lit("*** "),
                     F.substring("last_name", 1, 1), F.lit("**")))
        .select("customer_id","full_name_masked","state","country","gender","source"))

    return (masked
        .join(rfm.select("customer_id","recency_days","frequency","monetary","segment"),
              "customer_id", "left")
        .join(risk.select("customer_id","risk_level","risk_score"), "customer_id", "left")
        .join(priority.select("customer_id","priority"), "customer_id", "left")
        .fillna({"segment": "No Purchase", "risk_level": "Unknown", "priority": "N/A"}))


def main():
    spark = get_spark("gold_metrics")
    ensure_namespaces(spark)

    orders  = read_table(spark, "silver", "orders")
    tickets = read_table(spark, "silver", "tickets")
    product = read_table(spark, "silver", "product")
    crm     = read_table(spark, "silver", "crm")
    click   = read_table(spark, "silver", "clickstream")

    succ = orders.filter((F.col("status")=="success") &
                         F.col("order_date").isNotNull() & (F.col("order_amount")>0))

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

    pen = build_penetration(succ, product, rfm.count())
    write_table(pen, "gold", "category_penetration")
    print(f"== Category penetration == -> {target('gold','category_penetration')}")
    pen.show()

    funnel = build_funnel(click)
    write_table(funnel, "gold", "behavior_funnel")
    print(f"== Behaviour funnel == -> {target('gold','behavior_funnel')}")
    funnel.show()

    aband = build_cart_abandoners(click, succ)
    write_table(aband, "gold", "cart_abandoners")
    print(f"== Cart abandoners == -> {target('gold','cart_abandoners')}  {aband.count()} customers")

    c360 = build_customer_360(crm, rfm, risk, priority)
    write_table(c360, "gold", "customer_360")
    print(f"== Customer 360 == -> {target('gold','customer_360')}  {c360.count()} rows")
    c360.show(5, truncate=False)

    print("[Gold] metrics complete")
    spark.stop()


if __name__ == "__main__":
    main()

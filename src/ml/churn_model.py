"""Churn classifier: purchase, support and profile history -> no order in the next window.

The cutoff is AS_OF_DATE minus BEHAVIOUR_WINDOW_DAYS. A customer with a successful order
before the cutoff is churned if they place no successful order from the cutoff to
AS_OF_DATE. Features use only data dated before the date they are computed for, so one
function builds the training features (at the cutoff) and the scoring features (after
AS_OF_DATE).

  gold.churn_scores     churn probability per customer with a successful order
  metadata.ml_metrics   AUC of each model and of the rule-based risk score on the same
                        held-out customers, appended per run

The test set and the cross-validation folds are taken from a hash of customer_id, and
the data is partitioned and sorted by customer_id before training, so results do not
depend on how the input happens to be partitioned.
"""
import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from spark_session import get_spark, AS_OF_DATE, BEHAVIOUR_WINDOW_DAYS as W, RUN_ID
from lakehouse_io import read_table, write_table, ensure_namespaces, target
from gold_metrics import build_rfm, build_risk
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

SEED = 42
NUMERIC = ["recency_days", "frequency", "monetary", "avg_order_value", f"orders_{W}d",
           f"orders_{2 * W}d", "first_order_days", "distinct_categories", "bad_orders",
           "tickets_total", "negative_tickets", "open_tickets", "tenure_days"]
CATEGORICAL = ["gender", "source"]

METRICS_SCHEMA = ("run_id string, model string, auc_roc double, auc_pr double, cv_auc double, "
                  "train_rows bigint, test_rows bigint, test_churn_rate double, cutoff string, "
                  "as_of string, params string, used_for_scores boolean")


def successful(orders):
    """Successful orders, as counted in Gold."""
    return orders.filter((F.col("status") == "success") & F.col("order_date").isNotNull()
                         & (F.col("order_amount") > 0))


def features(orders, tickets, crm, product, ref_date):
    """One row per customer with a successful order before `ref_date`, from data dated
    before it only."""
    ref = F.to_date(F.lit(ref_date))
    succ = successful(orders).filter(F.col("order_date") < ref)
    purchases = (succ.join(product.select("product_id", "category"), "product_id", "left")
        .withColumn("category", F.coalesce(F.col("category"), F.lit("unknown")))
        .groupBy("customer_id").agg(
            F.datediff(ref, F.max("order_date")).alias("recency_days"),
            F.count("*").alias("frequency"),
            # rounded to cents as in Gold, so the sums do not depend on partition order
            F.round(F.sum("order_amount"), 2).alias("monetary"),
            F.round(F.avg("order_amount"), 2).alias("avg_order_value"),
            F.sum(F.when(F.col("order_date") >= F.date_sub(ref, W), 1).otherwise(0)).alias(f"orders_{W}d"),
            F.sum(F.when(F.col("order_date") >= F.date_sub(ref, 2 * W), 1).otherwise(0)).alias(f"orders_{2 * W}d"),
            F.datediff(ref, F.min("order_date")).alias("first_order_days"),
            F.countDistinct("category").alias("distinct_categories")))
    bad = (orders.filter(F.col("status").isin("refunded", "failed") & (F.col("order_date") < ref))
           .groupBy("customer_id").agg(F.count("*").alias("bad_orders")))
    support = (tickets.filter(F.to_date("created") < ref).groupBy("customer_id").agg(
        F.count("*").alias("tickets_total"),
        F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("negative_tickets"),
        F.sum(F.when(F.col("resolved").isNull() | (F.to_date("resolved") >= ref), 1)
               .otherwise(0)).alias("open_tickets")))
    profile = crm.select("customer_id", F.datediff(ref, "signup_date").alias("tenure_days"),
                         "gender", "source")
    return (purchases
        .join(bad, "customer_id", "left")
        .join(support, "customer_id", "left")
        .join(profile, "customer_id", "left")
        .fillna(0, ["bad_orders", "tickets_total", "negative_tickets", "open_tickets", "tenure_days"])
        .fillna({"gender": "unknown", "source": "unknown"}))


def pipeline(classifier):
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep",
                              stringOrderType="alphabetAsc") for c in CATEGORICAL]
    encoder = OneHotEncoder(inputCols=[f"{c}_idx" for c in CATEGORICAL],
                            outputCols=[f"{c}_vec" for c in CATEGORICAL], handleInvalid="keep")
    assembler = VectorAssembler(inputCols=NUMERIC + [f"{c}_vec" for c in CATEGORICAL], outputCol="raw")
    scaler = StandardScaler(inputCol="raw", outputCol="features")
    return Pipeline(stages=indexers + [encoder, assembler, scaler, classifier])


def feature_names(df, col="raw"):
    attrs = df.schema[col].metadata["ml_attr"]["attrs"]
    named = [a for group in attrs.values() for a in group]
    return [a["name"] for a in sorted(named, key=lambda a: a["idx"])]


def top(names, weights, n=8):
    ranked = sorted(zip(names, weights), key=lambda p: -abs(p[1]))[:n]
    return ", ".join(f"{name} {w:+.3f}" for name, w in ranked)


def main():
    spark = get_spark("churn_model")
    ensure_namespaces(spark)
    cutoff = spark.sql(f"SELECT date_sub(DATE '{AS_OF_DATE}', {W})").first()[0].isoformat()
    after_as_of = spark.sql(f"SELECT date_add(DATE '{AS_OF_DATE}', 1)").first()[0].isoformat()

    orders  = read_table(spark, "silver", "orders")
    tickets = read_table(spark, "silver", "tickets")
    product = read_table(spark, "silver", "product")
    crm     = read_table(spark, "silver", "crm")

    # ---- labelled data at the cutoff
    bought = (successful(orders)
        .filter((F.col("order_date") >= F.lit(cutoff)) & (F.col("order_date") <= F.lit(AS_OF_DATE)))
        .select("customer_id").distinct().withColumn("bought", F.lit(1)))
    data = (features(orders, tickets, crm, product, cutoff)
        .join(bought, "customer_id", "left")
        .withColumn("label", F.when(F.col("bought").isNull(), 1.0).otherwise(0.0)).drop("bought")
        .withColumn("is_test", F.pmod(F.xxhash64("customer_id"), F.lit(5)) == 0)
        .withColumn("fold", F.pmod(F.xxhash64(F.concat(F.col("customer_id"), F.lit("fold"))), F.lit(3)).cast("int"))
        .repartition(4, "customer_id").sortWithinPartitions("customer_id")
        .cache())

    # No order on or after the cutoff may reach the features.
    first_recency = data.agg(F.min("recency_days")).first()[0]
    if first_recency < 1:
        raise SystemExit(f"[ML] leakage: an order on or after the cutoff reached the features (min recency {first_recency})")

    train, test = data.filter(~F.col("is_test")), data.filter(F.col("is_test"))
    n_train, n_test = train.count(), test.count()
    churn_rate = test.agg(F.avg("label")).first()[0]
    print(f"[ML] cutoff={cutoff}  as_of={AS_OF_DATE}  population={n_train + n_test}  "
          f"churned={data.agg(F.sum('label')).first()[0]:.0f} ({data.agg(F.avg('label')).first()[0]:.1%})  "
          f"train={n_train}  test={n_test}  test churn rate={churn_rate:.1%}")

    # numBins=0: exact curves; binned curves depend on how Spark samples the sorted scores
    roc = BinaryClassificationEvaluator(metricName="areaUnderROC", numBins=0)
    pr = BinaryClassificationEvaluator(metricName="areaUnderPR", numBins=0)

    # ---- rule-based risk score from Gold, recomputed at the cutoff
    before = F.col("order_date") < F.lit(cutoff)
    rfm_cut = build_rfm(successful(orders).filter(before), as_of=cutoff)
    risk_cut = build_risk(rfm_cut, orders.filter(before), tickets.filter(F.to_date("created") < F.lit(cutoff)))
    rule = test.join(risk_cut.select("customer_id", F.col("risk_score").cast("double").alias("rule_score")),
                     "customer_id")
    if rule.count() != n_test:
        raise SystemExit("[ML] the rule-based score is missing for some test customers")
    rows = [(RUN_ID, "rule_baseline",
             roc.setRawPredictionCol("rule_score").evaluate(rule),
             pr.setRawPredictionCol("rule_score").evaluate(rule),
             None, n_train, n_test, churn_rate, cutoff, AS_OF_DATE, "risk_score from gold_metrics.build_risk", False)]
    roc.setRawPredictionCol("rawPrediction")
    pr.setRawPredictionCol("rawPrediction")

    # ---- models, tuned by 3-fold cross-validation on the training customers
    lr = LogisticRegression(maxIter=100)
    rf = RandomForestClassifier(seed=SEED)
    candidates = [
        ("logistic_regression", lr, ParamGridBuilder().addGrid(lr.regParam, [0.0, 0.01, 0.1]).build()),
        ("random_forest", rf, ParamGridBuilder().addGrid(rf.numTrees, [50, 100])
                                                .addGrid(rf.maxDepth, [5, 8]).build()),
    ]
    fitted = []
    for name, clf, grid in candidates:
        cv = CrossValidator(estimator=pipeline(clf), estimatorParamMaps=grid, evaluator=roc,
                            foldCol="fold", seed=SEED).fit(train)
        best = max(range(len(grid)), key=lambda i: cv.avgMetrics[i])
        cv_auc = float(cv.avgMetrics[best])
        params = ", ".join(f"{p.name}={v}" for p, v in grid[best].items())
        pred = cv.bestModel.transform(test)
        fitted.append((name, cv.bestModel, cv_auc, params, roc.evaluate(pred), pr.evaluate(pred)))

        names = feature_names(pred)
        last = cv.bestModel.stages[-1]
        weights = last.coefficients.toArray() if name == "logistic_regression" else last.featureImportances.toArray()
        print(f"[ML] {name:20s} cv auc={cv_auc:.4f}  {params}")
        print(f"     {'coefficients' if name == 'logistic_regression' else 'importance'}: {top(names, weights)}")

    # The scores come from the model family with the better cross-validated AUC.
    winner = max(fitted, key=lambda m: m[2])
    for name, _, cv_auc, params, auc_roc, auc_pr in fitted:
        rows.append((RUN_ID, name, auc_roc, auc_pr, cv_auc, n_train, n_test, churn_rate,
                     cutoff, AS_OF_DATE, params, name == winner[0]))
    metrics = spark.createDataFrame(rows, METRICS_SCHEMA)
    write_table(metrics, "metadata", "ml_metrics", mode="append")
    print(f"== Model evaluation on {n_test} held-out customers == -> {target('metadata', 'ml_metrics')}")
    metrics.select("model", F.round("auc_roc", 4).alias("auc_roc"), F.round("auc_pr", 4).alias("auc_pr"),
                   F.round("cv_auc", 4).alias("cv_auc"), "used_for_scores").show(truncate=False)

    # ---- score every customer on their history up to and including AS_OF_DATE
    current = features(orders, tickets, crm, product, after_as_of)
    scores = (winner[1].transform(current)
        .select("customer_id",
                F.round(vector_to_array("probability")[1], 4).alias("churn_probability"),
                F.lit(winner[0]).alias("model"),
                F.to_date(F.lit(AS_OF_DATE)).alias("scored_as_of")))
    write_table(scores, "gold", "churn_scores")
    scores = read_table(spark, "gold", "churn_scores")
    print(f"== Churn scores ({winner[0]}) == -> {target('gold', 'churn_scores')}  {scores.count()} customers")
    scores.select(F.round(F.avg("churn_probability"), 4).alias("mean"),
                  F.min("churn_probability").alias("min"),
                  F.max("churn_probability").alias("max")).show()

    print("[ML] churn model complete")
    spark.stop()


if __name__ == "__main__":
    main()

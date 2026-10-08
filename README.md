# Customer 360 Lakehouse

A medallion-architecture lakehouse consolidating five heterogeneous, dirty source
systems into a governed Customer 360 view and a set of analytics tables.

Apache Iceberg on MinIO, PySpark processing, Trino query layer, Superset dashboards,
running on Docker.

## Architecture

```
5 source CSVs
  -> Bronze   raw landing + audit columns + ingestion metadata
  -> Silver   cleaning + DQ validation + quarantine
  -> Gold     Customer 360 master table + RFM / risk / penetration / funnel metrics
  -> ML       churn classifier (Spark MLlib), evaluated against the rule-based risk score
  -> Superset dashboards, querying Iceberg through Trino
```

## Data sources

About 880k rows / 190 MB, containing unparseable timestamps, comma-separated amounts,
negative prices, inconsistent casing, multi-value fields and cross-field contradictions.

| Source | Rows | Role |
|---|---|---|
| orders | 300k | transaction facts |
| clickstream | 500k | behavioural events |
| crm | 50k | customer master |
| tickets | 30k | support facts |
| product | 500 | product dimension |

## Running

### A. Baseline (local Parquet, no Docker)

```bash
pip install -r requirements.txt     # pyspark 3.5.5, requires Java 8, 11 or 17
export LAKEHOUSE_MODE=local
export LOCAL_DATA=./data
export LOCAL_OUT=./warehouse
./run_pipeline.sh
```

Output lands in `./warehouse/{bronze,silver,quarantine,gold,metadata}/`.

`.env` is not read by any code; export the variables as shown. `.env.example`
documents them.

### B. Lakehouse (Iceberg + MinIO on Docker)

```bash
cd infra && docker compose up -d
docker compose exec spark-iceberg bash

cd /home/iceberg/project
export LAKEHOUSE_MODE=iceberg
export LOCAL_DATA=/home/iceberg/data
./run_pipeline.sh
```

Writes to `demo.{bronze,silver,quarantine,gold,metadata}.*`. Namespaces are created
by `lakehouse_io.ensure_namespaces()`.

Both modes run the same code. A table-by-table comparison of the two, covering all 23
tables and the run metadata, found them identical (`docs/run_log.md`, Runs 4 to 6).

Every time-based metric is measured from `AS_OF_DATE` (default 2025-12-01, the last
order date), and records dated after it are rejected. Behaviour metrics cover the
`BEHAVIOUR_WINDOW_DAYS` (default 90) before it. `run_pipeline.sh` tags each run with a
fresh `RUN_ID`.

The compose file starts Spark, the Iceberg REST catalog, MinIO, Trino and Superset;
see [infra/README.md](infra/README.md).

### Querying through Trino

```bash
cd infra && docker compose exec trino trino
trino> SELECT segment, count(*) FROM iceberg.gold.customer_360 GROUP BY 1;
```

Trino is read-only and hides the `crm` table in Bronze, Silver and quarantine, which
holds unmasked personal data.

### Dashboards

Open http://localhost:8088 and log in as `admin` / `admin`. Superset imports six
dashboards from `infra/superset/dashboards/` on its first start. Every chart is
specified in `docs/dashboard_blueprint.md`.

| Dashboard | Question |
|---|---|
| 1. Customer overview | who are our customers |
| 2. RFM segmentation | how customer value is distributed |
| 3. Churn risk and campaign priority | who to contact first |
| 4. Product and category penetration | which categories sell, and which customers they reach |
| 5. Behaviour and conversion | where customers drop out between browsing and buying |
| 6. Data quality | what cleaning rejected and flagged, and what that cost downstream |

### Individual layers

Silver reads Bronze tables, Gold and the churn model read Silver tables, so the order
is fixed.

```bash
python3 src/bronze_ingest.py    # CSV      -> bronze.* + metadata.ingestion
python3 src/silver_clean.py     # bronze.* -> silver.* + quarantine.* + metadata.{dq_summary,integrity}
python3 src/gold_metrics.py     # silver.* -> gold.*
python3 src/ml/churn_model.py   # silver.* -> gold.churn_scores + metadata.ml_metrics
```

## Gold tables

| Table | Content | Dashboard |
|---|---|---|
| `customer_360` | one row per customer: profile, purchases, support, behaviour in the last 90 days | all |
| `customer_rfm` | RFM scores 1 to 5 and segment; equal values share a score | 2 |
| `customer_risk` | churn risk score and level | 3 |
| `campaign_priority` | retention priority from value x risk | 3 |
| `category_penetration` | buyers per category; orders whose product was quarantined count as `unknown` | 4 |
| `behavior_funnel` | nested funnel over the 90-day window, with explicit `step_no` | 5 |
| `cart_abandoners` | added to cart in the 90-day window without an order in it | 5 |
| `churn_scores` | churn probability from the classifier, per customer with a successful order | 3 |

`customer_360` has 48,200 rows, one per customer, and carries `full_name_masked` only,
with no email, phone or address. Customers with no orders are labelled `No Purchase`.

## Churn model

`src/ml/churn_model.py` learns from orders, tickets and profile data before
2025-09-02 whether a customer orders again by 2025-12-01, and evaluates the
rule-based risk score of `customer_risk` on the same held-out customers.
`metadata.ml_metrics` records the result of every run.

| Score, 8,703 held-out customers | AUC-ROC |
|---|---|
| rule-based risk score | 0.496 |
| logistic regression | 0.512 |
| random forest | 0.518 |

None of them predicts churn on this dataset: 70.5% of customers churn, almost
regardless of how often or how recently they ordered before. See design document §9
step 6.

## Data quality

Checks work at four levels: field (parsing and normalisation), record (a row on its
own), entity (one row per key, one golden record per CRM customer) and cross-source
(reference date, signup order, orphans). A rule either rejects a row into
`quarantine.<source>` with its reasons in `_err`, or warns and records the reason in
the row's `_warn`. Adding a check means adding one line to a source's `rules` or
`warnings` list.

```python
rules = [
    rule_not_null("customer_id"),
    rule_positive("order_amount"),
    rule_after("order_date", AS_OF_DATE),
]
warnings = [rule_positive("quantity")]
```

Counts per source, severity and reason go to `metadata.dq_summary`, with the rows and
quarantined rows of each source; orphans per relationship, before and after cleaning,
go to `metadata.integrity`.

Measured on the current dataset, 36.7% of the 880,500 source rows are quarantined:
orders 39.0% (90,000 unparseable dates, 25,798 non-positive amounts, 12,830 NaN
amounts), clickstream 39.0% (150,553 missing `customer_id`, 40,171 unparseable
timestamps, 23,189 after the reference date), tickets 30.1%, product 18.0%, CRM 3.6%
(1,800 duplicate customer records). The source foreign keys are intact, but
quarantining 90 products leaves 33,022 orders without a valid product.

## Layout

```
src/
  spark_session.py     Spark configuration, AS_OF_DATE, behaviour window, RUN_ID
  lakehouse_io.py      read and write abstraction over both modes
  bronze_ingest.py     Bronze: ingestion + audit columns
  silver_clean.py      Silver: cleaning, golden record, quarantine, run metadata
  gold_metrics.py      Gold: seven analytics tables
  ml/churn_model.py    churn classifier: features, label, evaluation, scores
  dq/framework.py      rules, warnings, deduplication, summaries
docs/
  design_document.md   design, data findings, implementation plan
  dashboard_blueprint.md  chart-by-chart dashboard specification
  run_log.md           plan and results of every pipeline run
infra/
  docker-compose.yml   Spark, Iceberg REST catalog, MinIO, Trino, Superset
  trino/               Trino catalog and access rules
  superset/            Superset image, start-up script, exported dashboards
  LICENSE.upstream     upstream Apache-2.0 licence
  README.md            modifications, ports, startup
data/                  source CSVs, not version-controlled
sql/                   analytical queries
notebooks/             exploration
run_pipeline.sh        Bronze -> Silver -> Gold -> churn model
```

## Attribution

The lakehouse stack is based on
[tabular-io/docker-spark-iceberg](https://github.com/tabular-io/docker-spark-iceberg)
(Apache License 2.0). `infra/docker-compose.yml` is derived from the upstream file;
modifications are listed in its header comment and the upstream licence is retained
in `infra/LICENSE.upstream`.

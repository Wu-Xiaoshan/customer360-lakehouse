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

Both modes run the same code; row counts and DQ percentages must match.

The compose file currently starts Spark, the Iceberg REST catalog and MinIO. Trino
and Superset are added in a later step; see [infra/README.md](infra/README.md).

### Individual layers

Silver reads Bronze tables and Gold reads Silver tables, so the order is fixed.

```bash
python3 src/bronze_ingest.py    # CSV      -> bronze.* + metadata.ingestion
python3 src/silver_clean.py     # bronze.* -> silver.* + quarantine.*
python3 src/gold_metrics.py     # silver.* -> gold.*
```

## Gold tables

| Table | Content | Dashboard |
|---|---|---|
| `customer_rfm` | RFM quintiles and segment | 2 |
| `customer_risk` | churn risk score and level | 3 |
| `campaign_priority` | retention priority from value x risk | 3 |
| `category_penetration` | distinct customers per category | 4 |
| `behavior_funnel` | funnel steps with explicit `step_no` | 5 |
| `cart_abandoners` | added to cart, never purchased | 5 |
| `customer_360` | one row per customer, drill-down target | all |

`customer_360` carries `full_name_masked` only, with no email, phone or address.
Customers with no orders are labelled `No Purchase`.

## Data quality

Rejected rows are quarantined with an `_err` array of reasons. Adding a check means
adding one line to a source's `rules` list; `dq/framework.py` does not change.

```python
rules = [
    rule_not_null("customer_id"),
    rule_positive("order_amount"),
]
```

Measured on the current dataset, 33.9% of the 880,500 source rows are quarantined:
orders 39.0% (90,000 unparseable dates, 25,798 non-positive amounts, 12,830 NaN
amounts), clickstream 35.7% (150,553 missing `customer_id`, 40,171 unparseable
timestamps), tickets 28.3% (7,179 unparseable creation timestamps, 1,323 resolved
before created), product 18.0%, CRM 0% because its defects are normalised rather
than rejected.

## Layout

```
src/
  spark_session.py     Spark configuration, local / iceberg modes
  lakehouse_io.py      read and write abstraction over both modes
  bronze_ingest.py     Bronze: ingestion + audit columns
  silver_clean.py      Silver: cleaning, DQ, quarantine
  gold_metrics.py      Gold: seven analytics tables
  dq/framework.py      rule-based DQ framework
infra/
  docker-compose.yml   Spark, Iceberg REST catalog, MinIO
  LICENSE.upstream     upstream Apache-2.0 licence
  README.md            modifications, ports, startup
data/                  source CSVs, not version-controlled
sql/                   analytical queries
notebooks/             exploration
run_pipeline.sh        Bronze -> Silver -> Gold
```

## Attribution

The lakehouse stack is based on
[tabular-io/docker-spark-iceberg](https://github.com/tabular-io/docker-spark-iceberg)
(Apache License 2.0). `infra/docker-compose.yml` is derived from the upstream file;
modifications are listed in its header comment and the upstream licence is retained
in `infra/LICENSE.upstream`.

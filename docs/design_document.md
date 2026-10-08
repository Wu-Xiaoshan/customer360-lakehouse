# Design Document — Customer 360 Lakehouse Platform

> Final Project · ID2221 Data-Intensive Computing
> **Status: implementation in progress.** The pipeline runs end to end on Iceberg and produces a Customer 360 table with one row per customer, with identical results on local Parquet. Trino serves it read-only, six Superset dashboards show it, and a churn classifier is evaluated against the rule-based risk score. §9 step 7 is next. The plan and the division of work are in §9, the numbers from each run are in `run_log.md`, and the open items are in §10.

---

## 0. How to read this document

This is the internal design doc — more detailed than the 1-page proposal you submit for approval. Once approved, this doc guides implementation. The 1-page proposal is a compressed version of Sections 1–4 here (a proposal skeleton is at the very end).

---

## 1. Problem Description

**The problem.** In most customer-facing organizations, customer data is fragmented across independent systems: transactions in one system, CRM/interaction records in another, product catalog in a third, web/app behavior in a fourth. Each uses different schemas, formats, and update cadences. Because there is no unified view, business teams cannot answer basic cross-cutting questions — *which customers are at risk of leaving, and why?* — without slow, manual, cross-team data extraction.

**What we build.** A reusable data platform (a "lakehouse") that ingests these heterogeneous sources, cleans and integrates them into a single governed **Customer 360** view, and exposes it for analytics. On top of the integrated view we answer concrete business questions (customer segmentation, churn / at-risk identification).

**Why this is a valid Big Data problem.** This is the canonical "break the silos" data-engineering use case (the same reference scenario used by Databricks, Dremio, and others). It exercises the full stack this course targets: heterogeneous ingestion, distributed processing, an open table format on object storage, data-quality governance, and analytical serving — at a data volume where a distributed engine (Spark) is justified.

**Scope boundary.** The emphasis is the *platform and data-engineering pipeline*, not maximizing predictive accuracy. Analytical methods (and an optional ML model) sit on top to demonstrate that the integrated platform enables downstream analytics.

**What "Customer 360" requires.** Three properties, each tested in §9: exactly one row per real customer; every source contributing attributes to that row; and all sources measured against one reference date.

---

## 2. Tools (Technology Stack)

| Layer | Technology | Role | Why this choice |
|---|---|---|---|
| Object storage | **MinIO** | S3-compatible store for all data | Open-source, S3 API, the modern equivalent of HDFS; lightweight |
| Table format | **Apache Iceberg** | ACID tables, schema evolution, time travel, hidden partitioning | Open table format; engine-agnostic; the lakehouse standard |
| Catalog | **Iceberg REST catalog** | Tracks Iceberg table metadata | Lightest option; official quickstart default; avoids running a separate Hive Metastore + DB |
| Processing | **Apache Spark (PySpark)** | Ingestion, cleaning, integration, feature building, analytics | Course core; the required big-data processing framework |
| Query | **Trino** | Interactive SQL over Iceberg | Serves Superset; MPP engine (equivalent to Impala) |
| Visualization | **Superset** | Dashboards over the Gold tables | Required deliverable (visualization tool); connects to Trino over Iceberg |
| ML (optional) | **Spark MLlib** | Churn classification | Only if we add the predictive component |
| Runtime | **Docker Compose** | Container orchestration | Runs on any machine with Docker; avoids K8s complexity |

**Where it runs.** On a team member's laptop, inside WSL2 (Ubuntu 24.04) with Docker Engine 29.8.1 and Compose v5.5.1. The repository and the source CSVs live on the Linux filesystem at `~/customer360-lakehouse`; the Iceberg tables live in MinIO inside the stack.

The KTH and Aalto shared Linux servers were checked first and cannot run this stack: `brute.aalto.fi` has no Docker, no Podman and no subordinate UID range, so no container runtime can be installed without administrator help; only `apptainer` is available as a module, which runs single images but not Compose. kthcloud offers free VMs, but the default account tier has no `useVms` permission and a 20 GB disk quota, both of which need a tier upgrade. Running on WSL2 costs nothing and, because everything is defined in `infra/docker-compose.yml`, the same stack starts unchanged on a cloud VM (CSC Pouta or kthcloud) if a shared environment becomes necessary.

**Pinned versions.** `tabulario/spark-iceberg:3.5.5_1.8.1` (Spark 3.5.5, Iceberg 1.8.1, Python 3.10, Java 17) and `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`. `requirements.txt` pins `pyspark==3.5.5` so that local runs match the container.

**Persistence.** The upstream quickstart wipes the `warehouse` bucket on every start and keeps the REST catalog in memory. Our compose file removes the wipe, puts the catalog in a SQLite file, and gives MinIO and the catalog named volumes, so tables survive `docker compose down` and a restart.

**Starting point.** The official Apache Iceberg *Spark and Iceberg Quickstart* (`tabular-io/docker-spark-iceberg`) provides a verified compose file (Spark + REST catalog + MinIO). We extend it rather than build from zero.

---

## 3. Data

**Dataset.** Five CSV files representing five heterogeneous source systems, 880,500 rows and 179 MB in total, from the public Kaggle dataset "Customer 360" (`https://www.kaggle.com/datasets/vinaykandimalla/customer-360`). `[TODO] Confirm on the dataset page that the licence permits coursework use, and cite it in the proposal and the report.`

| Source file | Rows | Size | Source system | Role | Schema (columns) |
|---|---|---|---|---|---|
| `clickstream_500k_events.csv` | 500,000 | 122 MB | Web/App analytics | Fact (largest) | event_id, session_id, customer_id, event_type, page_url, device_id, timestamp, ingest_run_id |
| `orders_300k_dirty.csv` | 300,000 | 35 MB | Order/transaction system | Fact | order_id, customer_id, product_id, order_amount, order_date, payment_method, status, quantity |
| `crm_50000_customers_dirty_v3.csv` | 50,000 | 11 MB | CRM | Customer dimension (master) | customer_id, first_name, last_name, email, phone_number, gender, dob, signup_date, address, city, state, country, device_id(s), source |
| `support_tickets_30000_dirty.csv` | 30,000 | 4.2 MB | Support system | Fact | ticket_id, customer_id, issue_type, ticket_created, ticket_resolved, resolution_time_hours, sentiment, support_agent |
| `product_catalog_dirty_30pct.csv` | 500 | 19 KB | Product catalog | Lookup dimension (small) | product_id, product_name, category, price |

**Join keys.** `customer_id` links CRM to orders, tickets and clickstream; `product_id` links orders to the catalogue. Both hold in the source data: no order, ticket or event references a customer or product absent from its Bronze parent. `device_id` and `session_id` look like keys but are not (§3.2).

**Collection.** Static CSV files, downloaded once into `data/`, which is mounted read-only into the Spark container at `/home/iceberg/data` as the source system landing zone. The CSVs are **not** uploaded to MinIO: MinIO holds the Iceberg tables from Bronze onwards. The read-only mount enforces that the pipeline never writes back to its sources. No live ingestion.

**Time coverage and reference date.** The sources cover different periods:

| Source | Span in the raw data |
|---|---|
| orders | 2023-12-03 → 2025-12-01 |
| clickstream | 2025-09-02 → 2028-02-10, 95.8% of it in the last four months of 2025 |
| tickets | 2023 → 2027 |
| CRM signup dates | 2013-12-02 → 2025-12-01 |

The pipeline therefore works against one **reference date, `AS_OF_DATE` = 2025-12-01**, the last order date. Records dated after it are rejected: 23,189 clickstream events and 1,845 tickets. Behaviour metrics use the **90 days before it** (2025-09-02 to 2025-12-01), the only period in which browsing and purchases overlap. Both values are set in `spark_session.py` and can be overridden by environment variable.

**Data-volume note.** 880k rows is well-sized for single-node Spark: large enough to justify distributed processing, small enough to run on a laptop.

### 3.1 Dirty-data catalogue

Measured on the data; the counts drive the rules in §4.2. The catalogue is organised by the level at which a defect can be seen, because a column-by-column inspection only finds the first two levels.

| Level | Source | Defect | Count |
|---|---|---|---|
| Field | clickstream | unparseable timestamps (`2024/31/01 25:61:00`) | 40,171 |
| Field | orders | unparseable `order_date` | 90,000 |
| Field | orders | `order_amount` with spaces or commas; NaN after casting | 12,830 NaN |
| Field | orders | dirty `payment_method` (`wall-et`, `crad`) that no rule can map | 13,016 |
| Field | product | truncated or garbled `category` (`clo`, `3l3ctronics`) | 30 unrecognisable |
| Field | CRM | names with punctuation (`Jamie!`) | ~3,100 per name field |
| Field | CRM | names with doubled letters (`KKeevvin`, `Ppaarrrish`) | ~1,500 per name field |
| Record | orders | non-positive `order_amount` | 25,798 |
| Record | orders | missing or non-positive `quantity` | 45,651 |
| Record | product | non-positive or NaN `price` | 71 |
| Record | tickets | unparseable `ticket_created` | 7,179 |
| Record | tickets | resolved before created | 1,323 |
| Record | tickets | `resolution_time_hours` disagrees with the timestamps by more than 1 h | 1,310 |
| Record | clickstream | missing `customer_id` | 150,553 |
| Entity | CRM | duplicated `customer_id`: 1,734 ids, 1,800 extra rows, 481 ids with conflicting content | 1,800 |
| Cross-source | clickstream, tickets | dated after the reference date | 23,189 / 1,845 |
| Cross-source | orders, tickets | dated before the customer's signup | 14,922 / 1,711 |
| Cross-source | tickets | resolved after the reference date | 1,414 |

### 3.2 Findings verified in the data

These are conclusions, not open questions, and each changed what the pipeline does or does not do.

- **Shared contact details are different people, so ids are not merged.** 450 emails and 500 phone numbers are shared by more than one `customer_id`. The records under one address are unrelated people: `shared1459@hotmail.com` covers Julie Woods, Robert Kelly, Jamie Black, Andrea Hale and Todd Harding, with different phones and signup dates between 2015 and 2025. Merging on email would fuse them, so the pipeline deduplicates on `customer_id` only.
- **Source foreign keys are intact.** No order, ticket or event references a missing customer or product in Bronze.
- **Our own quarantine creates orphans.** Rejecting 90 of the 500 products leaves 33,022 orders without a valid product, and those orders belong to 42.3% of buying customers. Gold counts them as category `unknown` instead of dropping them.
- **`device_id` is not a key.** 0 of 305,114 clickstream device ids appear in the CRM.
- **`session_id` is not a session.** 8,000 distinct values are spread over 305,114 events; each is shared by 38 customers on average and none belongs to a single customer. It cannot group a customer's events, so no metric uses it.

**On data realism (report framing).** The dataset is synthetic-but-realistic and deliberately messy. We frame the project around *platform capability* (integration + governance), not "real" business insights, so the synthetic nature is a feature (controlled, reproducible, rich in data-quality challenges), not a weakness.

---

## 4. Methodology and Algorithm

We follow a **Medallion architecture** (Bronze → Silver → Gold), a standard layered lakehouse pattern, plus analytical methods on top. None of these methods is ML-dependent; ML is an optional add-on.

### 4.1 Bronze — raw landing
- Ingest each source file as-is into Iceberg tables, source-aligned, all columns as strings.
- Two changes only: column names normalised to snake_case (`device_id(s)` → `device_ids`) and audit columns `_source`, `_ingest_ts`, `_ingest_run_id`.
- Row counts and durations per source are appended to `metadata.ingestion` under a `run_id` shared by all three stages of a run.

### 4.2 Silver — clean, validate, deduplicate

Checks work at four levels, matching §3.1.

| Level | What it checks | Examples |
|---|---|---|
| Field | values inside one cell: parsing, normalisation | timestamps and numbers parsed with `try_*`; categories, status, sentiment mapped to canonical sets; names stripped of punctuation and title-cased; `device_ids` exploded into `silver.customer_device` |
| Record | one row on its own | not-null keys; positive amounts and prices; NaN; resolved not before created; quantity |
| Entity | several rows describing the same thing | one row per primary key on every source; one golden record per CRM customer |
| Cross-source | a row against another source | dated after the reference date; dated before the customer's signup; orphans per relationship |

**Two severities.** A rule either *rejects* a row into `quarantine.<source>` with its reasons in `_err`, or *warns*, keeping the row and recording the reason in `_warn`. Rejection is for rows that would make a metric wrong; warnings are for defects that do not affect any metric or where the data cannot say which side is wrong.

| Rejected | Warned |
|---|---|
| missing keys, unparseable dates and amounts, NaN, non-positive amounts and prices, resolved before created, dated after the reference date, duplicate primary keys | missing or non-positive quantity (then set to null), order or ticket before signup, unmappable payment method, doubled-letter names, ticket resolved after the reference date, resolution time mismatch |

Orders and tickets before signup are warnings because a guest order can precede account creation, so the signup date may be the wrong side. Quantity is a warning because no metric uses it, and rejecting a quarter of all orders over it would distort every purchase metric.

**Golden record.** Where one `customer_id` has several CRM rows, the pipeline keeps the row with the fewest warnings, then the most populated profile, then the lowest hash of its business columns. The last criterion makes reruns choose the same row. In practice it keeps `Parrish` over `Ppaarrrish`, because the corrupted name raises a warning.

**Framework.** A rule is a `(name, condition)` pair; each source declares its rule and warning lists. `dedup(df, key, order_by)` implements the entity level with a window function. Cross-source checks join the needed column from the other source first, so they remain ordinary single-table rules.

**Outputs.** `silver.<source>` keeps valid rows plus their `_warn` array. `metadata.dq_summary` holds one row per run, source, severity and reason, together with the source's row count and quarantined row count. `metadata.integrity` holds, per relationship, the orphan count against the Bronze parent and against the Silver parent; the difference between the two is the orphaning caused by our own quarantine.

### 4.3 Gold — integrate into Customer 360

Every time-based metric is measured from `AS_OF_DATE`; behaviour metrics cover the 90-day window before it.

**`customer_360`, one row per customer (48,200).**

| Group | Columns | From |
|---|---|---|
| Profile | customer_id, full_name_masked, state, country, gender, source, tenure_days | CRM |
| Purchases | recency_days, frequency, monetary, segment, bad_orders, favourite_category | orders, product |
| Support | tickets_total, open_tickets, negative_tickets, last_issue_type | tickets |
| Behaviour, last 90 days | events_90d, cart_events_90d, last_event_time | clickstream |
| Derived | risk_score, risk_level, priority | all of the above |

Email, phone and address never reach this table. Customers without orders are kept with segment `No Purchase`; count columns default to 0.

**Analytical tables.**

- **RFM segmentation**: recency, frequency and monetary per customer over successful orders, each scored 1 to 5, with the combination of R and F mapped to five segments. Customers with the same value always get the same score. Recency and monetary are quintiles from `percent_rank`, which gives tied values one rank. Frequency is the order count capped at 5: it takes only 15 values, mostly 1 to 4, so any split into five equal counts would give customers with the same number of orders different scores, while the counts 1, 2, 3, 4 and 5+ already hold 18.5, 25.5, 23.2, 16.2 and 16.6% of buyers.
- **Churn risk**: 0 to 2 points each for time since the last order, refunded or failed orders, and negative tickets; the sum maps to four levels. On this dataset the score does not predict churn (§9 step 6).
- **Campaign priority**: monetary quintile combined with the risk score.
- **Category penetration**: distinct buyers per category, with orders whose product was quarantined counted under `unknown`.
- **Conversion funnel**: page_view → search → add_to_cart → purchase inside the 90-day window. Each step intersects the previous step's customer set, so counts always decrease and the gap between steps is the loss.
- **Cart abandoners**: customers who added to cart in the window and placed no successful order in it.

### 4.4 Predictive component (ML)
Optional in the brief; implemented in `src/ml/churn_model.py`, stage 4 of `run_pipeline.sh`.
- **Task:** binary classification — will a customer who has bought before place no order in the next 90 days.
- **Label:** the data has none. With `AS_OF_DATE` = 2025-12-01 and a 90-day horizon, the cutoff is 2025-09-02. The population is every customer with a successful order before the cutoff; a customer is churned if they place no successful order from the cutoff to the reference date.
- **Features:** computed at a reference date from data dated before it only: recency, frequency, monetary, average order value, orders in the last 90 and 180 days, days since the first order, distinct categories, refunded or failed orders, tickets, negative tickets, open tickets, tenure, gender and acquisition channel. The clickstream starts on the cutoff date, so it cannot contribute features. The same function builds the training features at the cutoff and the scoring features after the reference date, and the program stops if an order on or after the cutoff reaches the training features.
- **Method:** Spark MLlib `Pipeline` — `StringIndexer` → `OneHotEncoder` → `VectorAssembler` → `StandardScaler` → `LogisticRegression` or `RandomForestClassifier`, each tuned by 3-fold cross-validation on AUC. One customer in five, chosen by a hash of `customer_id`, is held out. The Gold risk score, recomputed at the cutoff, is evaluated on the same customers as a baseline.
- **Reproducibility:** the test set and the folds come from hashes of `customer_id`, the data is partitioned and sorted by `customer_id` before training, and every seed is fixed, so the results do not depend on how the input is partitioned.
- **Outputs:** `gold.churn_scores`, the probability from the model with the better cross-validated AUC for every customer with a successful order; `metadata.ml_metrics`, AUC-ROC and AUC-PR of the two models and the baseline, appended per run.

### 4.5 Serving / presentation
- Analytical results exposed via Spark SQL and Trino.
- Six Superset dashboards specified in `dashboard_blueprint.md`.

---

## 5. Architecture

### 5.1 Data flow

```
 5 source CSV files (multi-source, dirty)
   in data/, mounted read-only into the Spark container
        │  read by Bronze only
        ▼
 ┌──────────────┐
 │    MinIO     │  ← S3-compatible object storage (Bronze and above live here)
 └──────┬───────┘
        │  read/write via Spark
        ▼
 ┌─────────────────────────────────────────────┐
 │            Apache Spark (PySpark)            │
 │  Bronze → Silver → Gold  +  DQ + (opt) MLlib │
 └──────┬──────────────────────────────────────┘
        │  Iceberg tables (metadata via REST catalog)
        ▼
 ┌──────────────┐        ┌───────────────────────┐
 │ REST Catalog │        │  Query / Presentation │
 │ (Iceberg md) │        │  Spark SQL / Trino     │
 └──────────────┘        │  → Superset dashboard  │
                         └───────────────────────┘
```

### 5.2 Scope decisions

To fit the project scope and the target runtime, the platform reads files from a read-only landing directory rather than live source databases, and uses a shell script (`run_pipeline.sh`) rather than a dedicated orchestrator. Small dimension tables (product_catalog) are not partitioned; large fact tables are partitioned by date.

Resulting container footprint: MinIO + `mc` (bucket setup) + Spark/Jupyter + REST catalog + Trino + Superset.

### 5.3 References

- Apache Iceberg — *Spark and Iceberg Quickstart* (`tabular-io/docker-spark-iceberg`).
- Medallion (Bronze/Silver/Gold) lakehouse architecture pattern.
- Customer 360 / churn reference use case (as described in Databricks and Dremio materials).

## 6. Storage & Naming Conventions

- **Bucket:** `s3://warehouse/` in MinIO, the default of the Iceberg quickstart.
- **Table naming:** catalog `demo`, one namespace per layer: `demo.bronze.orders`, `demo.silver.orders`, `demo.quarantine.orders`, `demo.gold.customer_360`. Namespaces are created by `lakehouse_io.ensure_namespaces()`.
- **Run metadata** lives in `demo.metadata`, appended once per run and keyed by `run_id`: `ingestion` (rows and seconds per source), `dq_summary` (rows per source, severity and reason, next to the source's total and quarantined rows), `integrity` (orphans per relationship).
- **Partitioning:** partition large fact/behavioral tables by date (e.g., `order_date` month); do **not** partition small dimension/lookup tables (products, customers) — low cardinality, would create small-file overhead.
- **Common data model:** ISO-8601 UTC timestamps; snake_case column names; explicit types; `_err` and `_warn` arrays for rule outcomes.

---

## 7. Repository Structure

```
customer360-lakehouse/          # lives at ~/customer360-lakehouse inside WSL
├── README.md                 # how to run (satisfies deliverable)
├── requirements.txt          # pyspark==3.5.5, matching the container
├── run_pipeline.sh           # Bronze → Silver → Gold → churn model under one RUN_ID
├── .gitignore                # ignores data/*.csv, warehouse/, .env
├── .env.example              # recognised variables, including AS_OF_DATE
├── docs/
│   ├── design_document.md    # this document (and a Chinese copy)
│   ├── dashboard_blueprint.md# dashboard build specification (and a Chinese copy)
│   └── run_log.md            # plan and results of every pipeline run
├── infra/
│   ├── docker-compose.yml    # MinIO + mc + Spark + REST catalog + Trino + Superset
│   ├── trino/                # Trino catalog and access rules
│   ├── superset/             # Superset image, start-up script, exported dashboards
│   ├── LICENSE.upstream      # Apache-2.0 licence of the quickstart we extend
│   └── README.md             # modifications to upstream, ports, startup
├── src/
│   ├── spark_session.py      # Spark config, AS_OF_DATE, behaviour window, RUN_ID
│   ├── lakehouse_io.py       # read/write abstraction over both modes
│   ├── bronze_ingest.py      # generic ingestion → Bronze
│   ├── silver_clean.py       # four-level cleaning, golden record, quarantine, run metadata
│   ├── gold_metrics.py       # Customer 360 + 6 analytical tables
│   ├── dq/framework.py       # rules, warnings, dedup, summaries
│   └── ml/churn_model.py     # MLlib churn classifier, stage 4
├── notebooks/                # exploration, analysis, charts
├── sql/                      # analytical queries
└── data/                     # ❌ CSVs gitignored (clickstream alone is 122 MB)
```

---

## 8. Evaluation Plan

To support the report with measurements (not intuition), measure:
- Ingestion time per source; total pipeline time.
- Storage size per layer; number of files generated.
- Query latency for the key analytical queries.
- DQ statistics per source, severity and reason (`metadata.dq_summary`).
- Orphans per relationship before and after cleaning (`metadata.integrity`).
- (If two partitioning strategies are compared on the fact table) ingestion time, storage, query latency, file count.
- (If ML) model AUC on a held-out test split.

---

## 9. Implementation Plan

Each step is independently verifiable, so that a failure is traceable to a single step. Status as of the last edit of this document.

### Step 1 — Environment · DONE

WSL2 (Ubuntu 24.04) on a laptop, Docker Engine 29.8.1, Compose v5.5.1. `cd infra && docker compose up -d` starts `spark-iceberg`, `rest`, `minio`, `mc`, from step 4 `trino` and from step 5 `superset`. Tables persist across restarts (§2).

### Step 2 — Run the pipeline on Iceberg · DONE

```bash
cd infra && docker compose up -d
docker compose exec spark-iceberg bash
cd /home/iceberg/project
export LAKEHOUSE_MODE=iceberg LOCAL_DATA=/home/iceberg/data
./run_pipeline.sh
```

The first run exposed three defects, fixed in Run 2: NaN amounts passing both the parse and the positivity check; product categories left with 23 distinct values for a catalogue of 8; and a funnel whose steps were not nested.

### Step 2b — Customer 360 completeness · DONE

An audit against the three properties in §1 found that the pipeline delivered none of them: 50,000 rows for 48,200 customers, no behaviour or support columns in `customer_360`, and no reference date, so the funnel compared three months of browsing with two years of orders. Run 3 added the entity and cross-source levels of §4.2, the reference date and window, and the full `customer_360`.

Run 3 took 4 min 18 s. 880,500 source rows became 557,646 valid rows and 322,854 quarantined rows (36.7%).

| Source | Valid | Quarantined | Rate |
|---|---|---|---|
| orders | 182,946 | 117,054 | 39.0% |
| clickstream | 305,114 | 194,886 | 39.0% |
| tickets | 20,976 | 9,024 | 30.1% |
| product | 410 | 90 | 18.0% |
| CRM | 48,200 | 1,800 | 3.6% |

Verified by thirteen checks, all passing: `customer_360` rows equal distinct ids equal Silver CRM rows (48,200); the golden record keeps `Parrish`; nothing in Silver is dated after the reference date; Silver plus quarantine equals Bronze for every source; the funnel decreases; category penetration has an `unknown` bucket; 99.8% of customers have behaviour in the window. Full numbers are in `run_log.md`.

### Step 3 — Cross-check against the local baseline · DONE

The same code ran in local mode, writing Parquet to `/tmp/c360_local` inside the Spark container, and every table was compared with its Iceberg counterpart: equal row counts, `exceptAll` empty in both directions on every non-floating column, and floating columns within 0.01 by key. All 26 comparisons are identical, covering 23 tables and the latest `dq_summary`, `integrity` and `ingestion` batches; the largest difference in `monetary` is 0.0. The storage abstraction changes no outcome.

A mismatch can come from the storage layer or from a computation that depends on row order, and preparing the cross-check exposed one of the second kind. `NTILE(5)` placed tied customers on either side of a quintile boundary by arrival order: rebuilding RFM from the same rows after `repartition(16)` changed the frequency score of 3,028 customers. Breaking ties on `customer_id` made the result repeatable but not fair, as 29,143 customers with the same order count still received different scores. RFM now scores ties equally (§4.3), and the cross-check was repeated on the final code. Details are in Runs 4 and 5 of `run_log.md`.

### Step 4 — Add Trino · DONE

Trino 483 runs as the `trino` service on host port 8085, with one catalog, `iceberg`, on the REST catalog and MinIO. The configuration is in `infra/trino/`. File-based access control makes the catalog read-only and gives no privileges on `crm` in `bronze`, `silver` and `quarantine`, which hold unmasked email, phone and address: the three tables are absent from `SHOW TABLES` and `information_schema`, and queries on them are denied. The other Silver tables contain no personal data and some dashboards read them.

Verified from the Trino CLI: `customer_360` has 48,200 rows and 48,200 distinct ids; segment counts, the funnel and the latest `integrity` batch equal the Spark results; a `DELETE` is denied; snapshots are listed through `"customer_rfm$snapshots"`, which the REST catalog rejects when Spark requests them.

Median latency over three runs of four dashboard queries: segment counts on `customer_360` 213 ms, the funnel 251 ms, customers and average monetary by country 315 ms, revenue by category joining `silver.orders` with `silver.product` 828 ms.

Read through Trino, the reject counts in `metadata.dq_summary` add up to more than the rows quarantined, because a row can fail several rules. `dq_summary` now also records `quarantined_rows` for each source (Run 6), and Dashboard 6 computes its rejection rate from it.

### Step 5 — Add Superset and build the dashboards · DONE

Superset 6.1.0 runs as the `superset` service on port 8088, built with the Trino driver and connected through `trino://superset@trino:8080/iceberg`. The six dashboards of `dashboard_blueprint.md`, 33 charts on 14 datasets, are exported to `infra/superset/dashboards/` and imported when Superset starts on an empty volume. Eight datasets are Gold or Silver tables. Six are SQL: the latest batch of each metadata table, revenue by category, events by weekday and hour, and drop-off per funnel step.

Verified in a headless browser on an instance started from an empty volume. Every chart renders without error, and the figures match `run_log.md`:
- 48,200 customers, 44,937 with orders
- the Run 5 segment, risk and priority counts
- eight categories plus `unknown`
- the funnel from 47,055 to 5,320
- rejection rates of 39.0, 39.0, 30.1, 18.0 and 3.6%

Drill to detail opens the `customer_360` rows behind a chart, and the Customer ID filter narrows *Customer lookup* to one customer. No dataset has an email, phone or address column.

Median load time over three loads, until every chart has rendered: 4.7 s for Dashboard 1, which holds the 48,200-row lookup table, and 2.5 to 3.0 s for the others.

### Step 6 — Churn classifier · DONE

`src/ml/churn_model.py`, stage 4 of `run_pipeline.sh` (§4.4). Of the 43,683 customers with a successful order before 2025-09-02, 70.5% place none in the following 90 days. On the 8,703 held-out customers:

| Score | AUC-ROC | AUC-PR |
|---|---|---|
| rule-based risk score, recomputed at the cutoff | 0.496 | 0.702 |
| logistic regression | 0.512 | 0.706 |
| random forest | 0.518 | 0.705 |

The standard error of these AUCs is about 0.007. The rule-based score therefore does no better than chance, and the models only slightly better; AUC-PR is close to the 70.1% churn rate of the test set for all three.

The data explains why:
- churn hardly varies with the number of earlier orders (71.5% after one, 68.9% after five)
- nor with the time since the last order (69.8 to 71.5%)
- successful orders are spread evenly over time, as if each were assigned to a random customer

The risk levels and campaign priorities of Dashboard 3 are therefore descriptive rather than predictive on this dataset, and the report should say so.

The results are identical in local and iceberg mode, 28 of 28 tables, after two fixes found by the cross-check: money features rounded to cents, and exact AUC curves instead of Spark's binned default. Stage 4 takes about 3 minutes. Details are in Run 7 of `run_log.md`.

### Step 7 — Integration and deliverables

A fresh clone plus `docker compose up -d` and `./run_pipeline.sh` must reproduce every number in the report. Write the README, the two-page report and the run instructions.

### Division of work

Steps 2 and 3 must happen before Steps 4 and 5, because Trino and Superset need Gold tables to read. After Step 3, the query and visualisation work can proceed in parallel with the optional model.

| Area | Owner | Depends on |
|---|---|---|
| Ingestion and Bronze | `[name]` | — |
| Cleaning, DQ rules, quarantine, golden record | `[name]` | Bronze |
| Gold integration and metrics | `[name]` | Silver |
| Trino and Superset, dashboards | `[name]` | Gold |
| Report and reproducibility | `[name]` | all |

---

## 10. Open Items

1. **Dataset licence** — confirm on the Kaggle page that it permits coursework use, and cite it.
2. `[TODO]` **Team split** — fill in the owners in the table above.
3. `[TODO]` **Examiner check** — confirm that extending the official Iceberg quickstart is acceptable and how to cite it. The modifications are already documented in the compose header and `infra/README.md`.
4. **Sessionisation (optional)** — `session_id` cannot group events (§3.2). Sessions could be rebuilt from each customer's event times, starting a new session after 30 minutes of inactivity; this is a standard windowed Spark job and would give Dashboard 5 a real session-depth chart.
5. **Scale test (optional)** — generate a copy of a few GB with new identifiers and measure how ingestion and cleaning scale.

---

## 11. One-Page Proposal Skeleton (for submission)

> Compress the above into ~1 page under the required four headings:

**1. Problem Description.** Customer data is siloed across systems; no unified view means teams can't identify at-risk customers or understand churn drivers. We build a lakehouse platform that integrates heterogeneous customer data into a governed Customer 360 view and performs segmentation and churn/at-risk analysis.

**2. Tools.** MinIO (S3 storage) · Apache Iceberg (table format) · Apache Spark/PySpark (processing) · Iceberg REST catalog · Trino (query) · Superset (visualization) · Docker Compose.

**3. Data.** Five heterogeneous source files (~880k rows / ~180 MB) representing distinct systems: web/app clickstream (500k), orders (300k), CRM customers (50k), support tickets (30k), product catalog (500), linked by customer_id and product_id. Dirty at the value level (invalid timestamps, non-standard categories, negative prices) and across records and sources (duplicate customer records, sources covering different periods). Static CSVs in a read-only landing directory; Iceberg tables from Bronze onwards. [Confirm dataset licence for coursework use.]

**4. Methodology and Algorithm.** Medallion architecture (Bronze→Silver→Gold): generic ingestion; cleaning at the field, record and entity level against a single reference date, with rejected rows quarantined and their reasons reported; integration of all five sources into a Customer 360 table with one row per customer; RFM segmentation, rule-based churn scoring, and a funnel and cart abandonment over the period where browsing and purchases overlap. Optional: Spark MLlib churn classifier with a label defined over the 90 days before the reference date, evaluated by AUC. Results served via Trino and visualized in Superset.

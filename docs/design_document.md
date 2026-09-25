# Design Document — Customer 360 Lakehouse Platform

> Final Project · ID2221 Data-Intensive Computing
> **Status: implementation started.** The runtime environment is settled and the stack runs; the pipeline has not been executed on Iceberg yet. The step-by-step plan and who does what is in §9. Remaining open items are in §10.

---

## 0. How to read this document

This is the internal design doc — more detailed than the 1-page proposal you submit for approval. Once approved, this doc guides implementation. The 1-page proposal is a compressed version of Sections 1–4 here (a proposal skeleton is at the very end).

---

## 1. Problem Description

**The problem.** In most customer-facing organizations, customer data is fragmented across independent systems: transactions in one system, CRM/interaction records in another, product catalog in a third, web/app behavior in a fourth. Each uses different schemas, formats, and update cadences. Because there is no unified view, business teams cannot answer basic cross-cutting questions — *which customers are at risk of leaving, and why?* — without slow, manual, cross-team data extraction.

**What we build.** A reusable data platform (a "lakehouse") that ingests these heterogeneous sources, cleans and integrates them into a single governed **Customer 360** view, and exposes it for analytics. On top of the integrated view we answer concrete business questions (customer segmentation, churn / at-risk identification).

**Why this is a valid Big Data problem.** This is the canonical "break the silos" data-engineering use case (the same reference scenario used by Databricks, Dremio, and others). It exercises the full stack this course targets: heterogeneous ingestion, distributed processing, an open table format on object storage, data-quality governance, and analytical serving — at a data volume where a distributed engine (Spark) is justified.

**Scope boundary.** The emphasis is the *platform and data-engineering pipeline*, not maximizing predictive accuracy. Analytical methods (and an optional ML model) sit on top to demonstrate that the integrated platform enables downstream analytics.

---

## 2. Tools (Technology Stack)

| Layer | Technology | Role | Why this choice |
|---|---|---|---|
| Object storage | **MinIO** | S3-compatible store for all data | Open-source, S3 API, the modern equivalent of HDFS; lightweight |
| Table format | **Apache Iceberg** | ACID tables, schema evolution, time travel, hidden partitioning | Open table format; engine-agnostic; the lakehouse standard |
| Catalog | **Iceberg REST catalog** | Tracks Iceberg table metadata | Lightest option; official quickstart default; avoids running a separate Hive Metastore + DB |
| Processing | **Apache Spark (PySpark)** | Ingestion, cleaning, integration, feature building, analytics | Course core; the required big-data processing framework |
| Query (optional) | **Trino** | Interactive SQL over Iceberg | Only if a BI/interactive layer is needed; MPP engine (equivalent to Impala) |
| Visualization | **Superset** | Dashboards over the Gold tables | Required deliverable (visualization tool); connects to Trino/Spark SQL over Iceberg |
| ML (optional) | **Spark MLlib** | Churn classification | Only if we add the predictive component |
| Runtime | **Docker Compose** | Container orchestration | Runs on any machine with Docker; avoids K8s complexity |

**Where it runs.** On a team member's laptop, inside WSL2 (Ubuntu 24.04) with Docker Engine 29.8.1 and Compose v5.5.1. The repository and the source CSVs live on the Linux filesystem at `~/customer360-lakehouse`; the Iceberg tables live in MinIO inside the stack.

The KTH and Aalto shared Linux servers were checked first and cannot run this stack: `brute.aalto.fi` has no Docker, no Podman and no subordinate UID range, so no container runtime can be installed without administrator help; only `apptainer` is available as a module, which runs single images but not Compose. kthcloud offers free VMs, but the default account tier has no `useVms` permission and a 20 GB disk quota, both of which need a tier upgrade. Running on WSL2 costs nothing and, because everything is defined in `infra/docker-compose.yml`, the same stack starts unchanged on a cloud VM (CSC Pouta or kthcloud) if a shared environment becomes necessary.

**Pinned versions.** `tabulario/spark-iceberg:3.5.5_1.8.1` (Spark 3.5.5, Iceberg 1.8.1, Python 3.10, Java 17) and `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`. `requirements.txt` pins `pyspark==3.5.5` so that local runs match the container.

**Starting point.** The official Apache Iceberg *Spark and Iceberg Quickstart* (`tabular-io/docker-spark-iceberg`) provides a verified compose file (Spark + REST catalog + MinIO). We extend it rather than build from zero.

---

## 3. Data

**Dataset (verified).** Five CSV files representing five heterogeneous source systems, ~880k rows / ~190 MB total, each linked by `customer_id` (and `product_id`, `device_id`). Multiple fields are **deliberately dirty**, which is the core reason this dataset fits the project: it directly exercises cleaning and data-quality governance.

| Source file | Rows | Size | Source system | Role | Schema (columns) |
|---|---|---|---|---|---|
| `clickstream_500k_events.csv` | 500,000 | 122 MB | Web/App analytics | Fact (largest) | event_id, session_id, customer_id, event_type, page_url, device_id, timestamp, ingest_run_id |
| `orders_300k_dirty.csv` | 300,000 | 35 MB | Order/transaction system | Fact | order_id, customer_id, product_id, order_amount, order_date, payment_method, status, quantity |
| `crm_50000_customers_dirty_v3.csv` | 50,000 | 11 MB | CRM | Customer dimension (master) | customer_id, first_name, last_name, email, phone_number, gender, dob, signup_date, address, city, state, country, device_id(s), source |
| `support_tickets_30000_dirty.csv` | 30,000 | 4.2 MB | Support system | Fact | ticket_id, customer_id, issue_type, ticket_created, ticket_resolved, resolution_time_hours, sentiment, support_agent |
| `product_catalog_dirty_30pct.csv` | 500 | 19 KB | Product catalog | Lookup dimension (small) | product_id, product_name, category, price |

**Join keys.** `customer_id` links CRM to orders/tickets/clickstream. `product_id` links orders to product_catalog. `device_id` links clickstream to CRM's multi-valued `device_id(s)`.

**Data-volume note.** ~880k rows is well-sized for single-node Spark: large enough to justify distributed processing and partition-pruning experiments, small enough to run on the server/laptop.

**Collection.** Static CSV files, downloaded once into `data/`, which is mounted read-only into the Spark container at `/home/iceberg/data` as the source system landing zone. The CSVs are **not** uploaded to MinIO: MinIO holds the Iceberg tables from Bronze onwards. The read-only mount enforces that the pipeline never writes back to its sources. No live ingestion.

**Verified dirty-data catalog (drives the Silver/DQ design in 4.2).**

| Source | Dirty pattern observed | Cleaning/DQ skill it exercises |
|---|---|---|
| clickstream | empty `customer_id`; invalid timestamps e.g. `2024/31/01 25:61:00`; CRLF line endings | missing-FK check; timestamp validation; format normalization |
| orders | bad `payment_method` (`wall-et` vs `wallet`); bad/empty `order_date`; trailing spaces in `order_amount`; outlier amounts | categorical standardization; timestamp validation; trim + cast; outlier detection |
| CRM | case chaos (`mARIA`, `KKeevvin`); leading spaces; many phone formats; multi-valued `device_id(s)` split by `;` | string standardization; trim; phone normalization; multi-value explode |
| product_catalog | truncated/garbled `category` (`clo`, `3l3ctronics`); negative `price` (`-100`); leading spaces | categorical cleaning; invalid-numeric validation; trim |
| support_tickets | empty `ticket_created`; invalid timestamps (`2025-12-12T28:77:10`); logical contradiction (resolved < created); `sentiment` case inconsistency; agent-name noise | missing-value check; timestamp validation; cross-field logical validation; standardization |

**Source.** Kaggle, "Customer 360" (`https://www.kaggle.com/datasets/vinaykandimalla/customer-360`). `[TODO] Confirm on the dataset page that the licence permits coursework use, and cite it in the proposal and the report.`

**On data realism (report framing).** The dataset is synthetic-but-realistic and deliberately messy. We frame the project around *platform capability* (integration + governance), not "real" business insights, so the synthetic nature is a feature (controlled, reproducible, rich in data-quality challenges), not a weakness.

---

## 4. Methodology and Algorithm

We follow a **Medallion architecture** (Bronze → Silver → Gold), a standard layered lakehouse pattern, plus analytical methods on top. None of these methods is ML-dependent; ML is an optional add-on.

### 4.1 Bronze — raw landing
- Ingest each source file as-is into Iceberg tables, source-aligned, no transformation.
- Capture ingestion metadata (row counts, ingest timestamp, source name, schema version).
- **Method:** generic PySpark ingestion (format-agnostic reader → Iceberg writer).

### 4.2 Silver — clean & standardize
Concrete cleaning tasks derived from the verified dirty-data catalog (Section 3):
- **String standardization:** case-normalize names/sentiment/categories; `trim` leading/trailing spaces (CRM names, order_amount, price, agent names).
- **Timestamp validation:** reject/repair invalid timestamps (`2024/31/01 25:61:00`, `2025-12-12T28:77:10`); normalize to one ISO-8601 UTC format.
- **Categorical standardization:** map dirty values to canonical set (`wall-et`->`wallet`; fix `clo`/`3l3ctronics`).
- **Numeric validation:** reject negative prices (`-100`); cast trimmed strings to numeric; flag outlier order amounts.
- **Multi-value handling:** explode CRM `device_id(s)` (`;`-separated) into a normalized customer-device link.
- **Missing / FK handling:** empty `customer_id` (clickstream), empty dates (orders/tickets).
- **Cross-field logical validation:** ticket `resolved` must be >= `created`.
- Invalid records are **isolated (quarantined) into a reject table, reported, and excluded** from Gold — not silently dropped.
- **Method:** rule-based validation framework — a set of reusable, composable check functions (generic checks like null/duplicate/type, plus per-source rules), so new rules are added with minimal code change.

### 4.3 Gold — integrate into Customer 360
- Join the cleaned sources into a single **Customer 360 table**: one row per customer enriched with transaction summaries, interaction history, product affinity, behavioral aggregates.
- Build analytical marts (segmentation, churn features).
- **Methods:**
  - **Aggregation & feature construction** (per-customer rollups: recency, frequency, monetary; interaction counts; tenure).
  - **Customer segmentation** — e.g., RFM-style segmentation or rule-based tiers. `【依据实际数据字段确定分群维度】`
  - **Churn / at-risk analysis** — rule-based scoring on the integrated features (no ML needed to satisfy the requirement).

### 4.4 Optional — Predictive component (ML)
*Include only if time permits; it is a bonus, not required.*
- **Task:** binary classification — predict whether a customer will churn.
- **Method:** Spark MLlib `Pipeline` — `VectorAssembler` → `StringIndexer`/`OneHotEncoder` → `StandardScaler` → classifier (e.g., `LogisticRegression` or `RandomForestClassifier`) → `Evaluator` (AUC).
- Reuses the Gold Customer 360 features as the training table.

### 4.5 Serving / presentation
- Analytical results exposed via Spark SQL (and optionally Trino for interactive queries).
- A **Superset dashboard** presenting: customer segments, churn-risk distribution, and key drivers.

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

Resulting container footprint: MinIO + `mc` (bucket setup) + Spark/Jupyter + REST catalog, plus Trino and Superset once §9 steps 4 and 5 are done.

### 5.3 References

- Apache Iceberg — *Spark and Iceberg Quickstart* (`tabular-io/docker-spark-iceberg`).
- Medallion (Bronze/Silver/Gold) lakehouse architecture pattern.
- Customer 360 / churn reference use case (as described in Databricks and Dremio materials).
- `【补充：具体参考的开源项目/教程链接】`

## 6. Storage & Naming Conventions

- **Bucket:** `s3://warehouse/` in MinIO, the default of the Iceberg quickstart.
- **Table naming:** catalog `demo`, one namespace per layer: `demo.bronze.orders`, `demo.silver.orders`, `demo.quarantine.orders`, `demo.gold.customer_360`, `demo.metadata.ingestion`. Namespaces are created by `lakehouse_io.ensure_namespaces()`.
- **Partitioning:** partition large fact/behavioral tables by date (e.g., `order_date` month); do **not** partition small dimension/lookup tables (products, customers) — low cardinality, would create small-file overhead.
- **Common data model:** ISO-8601 UTC timestamps; snake_case column names; explicit types; documented null-handling rules.

---

## 7. Repository Structure

```
customer360-lakehouse/          # lives at ~/customer360-lakehouse inside WSL
├── README.md                 # how to run (satisfies deliverable)
├── requirements.txt          # pyspark==3.5.5, matching the container
├── run_pipeline.sh           # Bronze → Silver → Gold
├── .gitignore                # ignores data/*.csv, warehouse/, .env
├── .env.example              # config template, no secrets
├── docs/                     # this design document
├── infra/
│   ├── docker-compose.yml    # MinIO + mc + Spark + REST catalog (+ Trino, Superset to come)
│   ├── LICENSE.upstream      # Apache-2.0 licence of the quickstart we extend
│   └── README.md             # modifications to upstream, ports, startup
├── src/
│   ├── spark_session.py      # Spark config, local / iceberg modes
│   ├── lakehouse_io.py       # read/write abstraction over both modes
│   ├── bronze_ingest.py      # generic ingestion → Bronze
│   ├── silver_clean.py       # cleaning + DQ + quarantine
│   ├── gold_metrics.py       # join → Customer 360 + 6 analytical tables
│   ├── dq/framework.py       # reusable validation rules
│   └── ml/ (optional)        # MLlib churn pipeline
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
- DQ statistics: rows rejected/quarantined per source.
- (If two partitioning strategies are compared on the fact table) ingestion time, storage, query latency, file count.
- (If ML) model AUC on a held-out test split.

---

## 9. Implementation Plan

Seven steps, each one independently verifiable, so that a failure is traceable to a single step. Status as of the last edit of this document.

### Step 1 — Environment · DONE

WSL2 (Ubuntu 24.04) on a laptop, Docker Engine 29.8.1, Compose v5.5.1. `cd infra && docker compose up -d` starts four services: `spark-iceberg`, `rest`, `minio`, `mc`.

Verified: four containers running, four mounts on the Spark container with `data/` read-only, Spark 3.5.5 with the Iceberg 1.8.1 runtime, the `warehouse` bucket created by `mc`, the REST catalog answering on 8181, and Jupyter (8888), the MinIO console (9001) and the Spark UI (8080) reachable from the host browser.

### Step 2 — Run the pipeline on Iceberg · NEXT

```bash
cd infra && docker compose up -d
docker compose exec spark-iceberg bash
cd /home/iceberg/project
export LAKEHOUSE_MODE=iceberg LOCAL_DATA=/home/iceberg/data
./run_pipeline.sh
```

Done when `demo.bronze.*`, `demo.silver.*`, `demo.quarantine.*` and `demo.gold.*` exist and the seven Gold tables are populated.

Record while it runs, because these numbers go straight into the report: rejection rate and reason counts per source, row counts per layer, and the per-source ingestion seconds stored in `demo.metadata.ingestion`.

### Step 3 — Cross-check against the local baseline

Run the same code with `LAKEHOUSE_MODE=local` and `LOCAL_OUT=./warehouse`, which writes Parquet instead of Iceberg through the same code path. Done when the row counts and DQ rates match Step 2. A mismatch means a bug in the storage abstraction, not in the business logic.

### Step 4 — Add Trino

Add a `trino` service to `infra/docker-compose.yml` with an Iceberg catalog pointing at the REST catalog (`http://rest:8181`) and MinIO (`http://minio:9000`). Done when `SELECT * FROM iceberg.gold.customer_360 LIMIT 10` returns rows from the Trino CLI. Add the new service to the modification list in the compose header comment, which the upstream Apache-2.0 licence requires.

### Step 5 — Add Superset and build the dashboards

Add `superset` to the compose file, install the Trino driver, and connect it to Trino. Then build the six dashboards described in the dashboard blueprint: customer overview, RFM segmentation, churn risk and campaign priority, product penetration, behaviour funnel, and data quality.

Done when every dashboard renders from the Gold tables. Export the dashboards from Superset into the repository so that a fresh instance can import them; dashboards live in Superset's own database and are lost otherwise.

### Step 6 — Churn classifier (optional)

Only if Steps 1 to 5 are finished with time to spare. Spark MLlib pipeline on the Gold feature table, reported as AUC. See §4.4.

### Step 7 — Integration and deliverables

A fresh clone plus `docker compose up -d` and `./run_pipeline.sh` must reproduce every number in the report. Write the README, the two-page report and the run instructions.

### Division of work

Steps 2 and 3 must happen before Steps 4 and 5, because Trino and Superset need Gold tables to read. After Step 3, the query and visualisation work can proceed in parallel with the optional model.

| Area | Owner | Depends on |
|---|---|---|
| Ingestion and Bronze | `[name]` | — |
| Cleaning, DQ rules, quarantine | `[name]` | Bronze |
| Gold integration and metrics | `[name]` | Silver |
| Trino and Superset, dashboards | `[name]` | Gold |
| Report and reproducibility | `[name]` | all |

---

## 10. Open Items

1. **Dataset licence** — confirm on the Kaggle page that it permits coursework use, and cite it. Tables, rows and schemas are already verified.
2. `【待确认】` **Team split** — fill in the owners in the table above.
3. `【待确认】` **Exact analysis questions** — the specific business questions the Gold layer answers (segmentation dimensions, churn definition).
4. `【待确认】` **Examiner check** — confirm that extending the official Iceberg quickstart is acceptable and how to cite it. The modifications are already documented in the compose header and `infra/README.md`.
5. **Optional components** — decide on the ML classifier once Step 5 is done. Trino and Superset are in scope for the query and visualisation requirement.

---

## 11. One-Page Proposal Skeleton (for submission)

> Compress the above into ~1 page under the required four headings:

**1. Problem Description.** Customer data is siloed across systems; no unified view means teams can't identify at-risk customers or understand churn drivers. We build a lakehouse platform that integrates heterogeneous customer data into a governed Customer 360 view and performs segmentation and churn/at-risk analysis.

**2. Tools.** MinIO (S3 storage) · Apache Iceberg (table format) · Apache Spark/PySpark (processing) · Iceberg REST catalog · Trino (query) · Superset (visualization) · Docker Compose.

**3. Data.** Five heterogeneous source files (~880k rows / ~190 MB) representing distinct systems: web/app clickstream (500k), orders (300k), CRM customers (50k), support tickets (30k), product catalog (500). Linked by customer_id / product_id / device_id. Deliberately dirty (invalid timestamps, inconsistent casing, bad categorical values, negative prices, multi-valued fields, cross-field contradictions), which exercises the cleaning and data-quality components. Static CSVs in a read-only landing directory; Iceberg tables from Bronze onwards. [Confirm dataset licence for coursework use.]

**4. Methodology and Algorithm.** Medallion architecture (Bronze→Silver→Gold): generic ingestion; rule-based cleaning + data-quality validation with quarantine; multi-source integration into Customer 360; aggregation + segmentation (RFM-style) + rule-based churn scoring. Optional: Spark MLlib churn classifier (`Pipeline` → LogisticRegression/RandomForest, AUC). Results served via Spark SQL/Trino and visualized in a Superset dashboard.

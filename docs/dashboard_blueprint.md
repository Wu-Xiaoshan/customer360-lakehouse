# Dashboard build specification

Six dashboards. Each chart names its source table, its fields, how it is computed and
what counts as done. Everything is SQL aggregation; no ML is involved.

Figures come from Run 2 on 2026-09-25; the full record is in `run_log.md`.

---

## Before starting

**Prerequisites**

| Item | Status | Note |
|---|---|---|
| Gold tables | done | seven tables in MinIO, listed below |
| Trino service | **not done** | design document §9 step 4; Superset queries Iceberg through it |
| Superset service | **not done** | design document §9 step 5 |
| `gold.dq_summary` table | **does not exist** | needed by Dashboard 6, see that section |

**Available sources**

| Table | Rows | Main fields |
|---|---|---|
| `gold.customer_360` | 50,000 | customer_id, full_name_masked, state, country, gender, source, recency_days, frequency, monetary, segment, risk_level, risk_score, priority |
| `gold.customer_rfm` | 44,937 | customer_id, recency_days, frequency, monetary, R, Fs, Ms, segment |
| `gold.customer_risk` | 44,937 | customer_id, recency_days, segment, bad_orders, neg_tickets, risk_score, risk_level |
| `gold.campaign_priority` | 44,937 | customer_id, segment, Ms, risk_score, priority |
| `gold.category_penetration` | 8 | category, customers, penetration_pct |
| `gold.behavior_funnel` | 4 | step_no, step, customers |
| `gold.cart_abandoners` | 2,032 | customer_id, cart_events |
| `silver.orders` | 182,946 | cleaned orders: status, payment_method, order_date, order_amount |
| `silver.clickstream` | 321,343 | cleaned events: event_type, event_time, session_id |
| `silver.crm` | 50,000 | cleaned master data: signup_date, dob |
| `silver.product` | 410 | cleaned catalogue: normalised category, price |
| `metadata.ingestion` | 5 rows per run | source, rows, seconds, ingested_at |

Prefer the Gold tables. Query Silver only where Gold does not cover the metric, and
say so in the chart description.

**Naming.** Superset datasets carry the table name, for example `gold_customer_360`.

---

## Dashboard 1 — Customer overview

Answers: who are our customers.

| Chart | Type | Source | Fields and computation |
|---|---|---|---|
| Total customers | big number | `gold.customer_360` | `count(*)`, expect 50,000 |
| Customers with orders | big number | `gold.customer_360` | `count(*) where segment <> 'No Purchase'`, expect 44,937 |
| Acquisition channel | pie | `gold.customer_360` | group by `source` |
| Gender split | pie | `gold.customer_360` | group by `gender` |
| Countries | bar, top 15 | `gold.customer_360` | group by `country`, descending |
| Sign-up trend | line | `silver.crm` | `signup_date` by month; Gold does not carry this field |

**Done when** both big numbers match the table above and no chart exposes email,
phone or address.

---

## Dashboard 2 — RFM segmentation

Answers: how customer value is distributed.

Recency, frequency and monetary are each scored into quintiles (`R`, `Fs`, `Ms`),
and the combination places each customer in one of five segments.

| Chart | Type | Source | Fields and computation |
|---|---|---|---|
| Customers per segment | bar | `gold.customer_rfm` | group by `segment`, descending |
| Average spend per segment | bar | `gold.customer_rfm` | `avg(monetary)` by `segment` |
| Average frequency per segment | bar | `gold.customer_rfm` | `avg(frequency)` by `segment` |
| Spend distribution | histogram | `gold.customer_rfm` | bucket `monetary` |
| High-value share | big number | `gold.customer_rfm` | share of customers with `Ms = 5`, 20% by construction |

Run 2 segments: Champions 11,858, Hibernating 11,603, Loyal 8,730, Others 6,373,
At Risk 6,373.

**Done when** the five segments sum to 44,937 and `monetary` contains no NaN.

---

## Dashboard 3 — Churn risk and campaign priority

Answers: who to contact first.

The risk score adds three signals, 0 to 2 points each: time since the last order,
refunded or failed orders, and negative-sentiment support tickets. The total, 0 to 6,
maps to four levels.

| Chart | Type | Source | Fields and computation |
|---|---|---|---|
| Risk level distribution | bar | `gold.customer_risk` | group by `risk_level` |
| Segment by risk | stacked bar | `gold.customer_risk` | group by `segment`, stack `risk_level` |
| What drives the score | bar | `gold.customer_risk` | `avg(bad_orders)` and `avg(neg_tickets)` by `risk_level` |
| Priority distribution | bar | `gold.campaign_priority` | group by `priority` |
| High-priority customers | table | `gold.customer_360` | `priority = 'HIGH'` ordered by `monetary`, showing masked name, segment, risk level, spend |

Run 2: risk Medium 28,429, High 10,025, Low 4,750, Very Low 1,733; priority
MEDIUM 28,763, HIGH 14,496, LOW 1,678.

**Done when** the table shows `full_name_masked` and no real names.

---

## Dashboard 4 — Product and category penetration

Answers: which categories sell, and which customers they have not reached.

| Chart | Type | Source | Fields and computation |
|---|---|---|---|
| Penetration by category | bar | `gold.category_penetration` | `penetration_pct` descending |
| Customers per category | bar | `gold.category_penetration` | `customers` descending |
| Revenue by category | bar | `silver.orders` join `silver.product` | sum `order_amount` by `category` where `status = 'success'`; no Gold table covers this |
| Payment methods | pie | `silver.orders` | group by `payment_method`: card, wallet, upi, cash, unknown |
| Repeat versus one-off | pie | `gold.customer_rfm` | `frequency = 1` against `frequency > 1` |

Run 2 penetration: clothing 30.8%, kitchen 29.6%, beauty 28.3%, home 26.7%,
sports 25.0%, automotive 24.7%, electronics 24.5%, toys 23.0%.

**Done when** exactly eight categories appear. Values such as `clo` or `clothing-`
mean the dashboard is reading data from before the category fix; re-run the pipeline.

---

## Dashboard 5 — Behaviour and conversion

Answers: where customers drop out between browsing and buying.

The funnel is nested: each step's customer set is a subset of the previous step, so
the counts always decrease and the gap between steps is the conversion loss.

| Chart | Type | Source | Fields and computation |
|---|---|---|---|
| Conversion funnel | funnel | `gold.behavior_funnel` | `customers` ordered by `step_no`. **Sort by `step_no`**, not by label |
| Drop-off per step | bar | `gold.behavior_funnel` | difference between consecutive steps over the earlier step |
| Cart abandoners | table | `gold.cart_abandoners` | ordered by `cart_events` descending |
| Activity by hour | heatmap | `silver.clickstream` | `event_time` by weekday and hour |
| Session depth | histogram | `silver.clickstream` | events per `session_id`, bucketed |

Run 2 funnel: page_view 47,260 → search 29,643 → add_to_cart 18,745 →
purchase 17,538, a drop of 37.3%, 36.8% and 6.4%.

**Done when** the four funnel bars decrease left to right and cart abandoners
shows 2,032 customers.

---

## Dashboard 6 — Data quality

Answers: what the cleaning layer rejected.

**Prerequisite: there is no Gold summary table yet.** The reasons live in the `_err`
array column of the `quarantine.*` tables, which Superset cannot group on directly.
`silver_clean.py` needs to persist the output of `dq_summary()` as
`gold.dq_summary` with columns `run_id, source, reason, count`. Do this before
building the dashboard.

| Chart | Type | Source | Fields and computation |
|---|---|---|---|
| Rejection rate per source | bar | `gold.dq_summary` | quarantined rows over Bronze rows |
| Reasons per source | stacked bar | `gold.dq_summary` | group by `source`, stack `reason` |
| Rows per layer | funnel or bar | layer tables | Bronze 880,500 → Silver 576,197, with 304,303 quarantined |
| Ingestion time | bar | `metadata.ingestion` | `seconds` by `source` for the latest `run_id` |

Run 2 rejection rates: orders 39.0%, clickstream 35.7%, tickets 28.3%,
product 18.0%, crm 0%.

**Done when** valid plus quarantined rows equal the Bronze row count for every source.

---

## Customer drill-down

Shared by all dashboards, always from `gold.customer_360`.

From any aggregate chart, open a single customer: masked name, geography,
acquisition channel, the three RFM values, segment, risk level and score, and
campaign priority.

**Masked fields only.** `gold.customer_360` excludes email, phone and address at the
point it is built, so no dashboard misconfiguration can leak them. State this in the
report.

---

## Build order and ownership

| Order | Work | Depends on | Owner |
|---|---|---|---|
| 1 | Add Trino, confirm it reads the Gold tables | Gold tables | `[name]` |
| 2 | Add Superset, connect to Trino, create datasets | Trino | `[name]` |
| 3 | Dashboards 2 and 3 | datasets | `[name]` |
| 4 | Dashboards 1 and 4 | datasets | `[name]` |
| 5 | Add `gold.dq_summary`, build Dashboard 6 | pipeline re-run | `[name]` |
| 6 | Dashboard 5 | datasets | `[name]` |

Dashboards 2 and 3 come first because they read Gold tables as they are, with no
further development, which makes them the quickest way to prove the Superset to
Iceberg path works.

**Export when finished.** Superset stores dashboards in its own database, not in
Iceberg. Export them to files and commit them, otherwise a fresh deployment starts
empty and the reproducibility requirement fails.

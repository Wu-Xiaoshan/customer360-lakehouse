# Dashboard build specification

Six dashboards, built in Superset and stored in `infra/superset/dashboards/`. Each
chart names its source, its fields, how it is computed and what counts as done. Everything is SQL aggregation; no ML is involved.

Figures come from Run 3 on 2026-09-26, and the RFM, segment and priority figures from
Run 5 on 2026-10-01; the full record is in `run_log.md`. Time-based
figures are measured from the reference date 2025-12-01, and behaviour figures cover
the 90 days before it.

---

## Before starting

**Prerequisites**

| Item | Status | Note |
|---|---|---|
| Gold and metadata tables | done | listed below |
| Trino service | done | design document §9 step 4; Superset connects with `trino://superset@trino:8080/iceberg` |
| Superset service | done | design document §9 step 5; http://localhost:8088, `admin` / `admin` |
| Hide the CRM tables in Trino | done | `crm` in `bronze`, `silver` and `quarantine` holds unmasked email, phone and address |

**Available sources**

| Table | Rows | Main fields |
|---|---|---|
| `gold.customer_360` | 48,200 | customer_id, full_name_masked, state, country, gender, source, tenure_days, recency_days, frequency, monetary, segment, bad_orders, favourite_category, tickets_total, open_tickets, negative_tickets, last_issue_type, events_90d, cart_events_90d, last_event_time, risk_score, risk_level, priority |
| `gold.customer_rfm` | 44,937 | customer_id, recency_days, frequency, monetary, R, Fs, Ms, segment |
| `gold.customer_risk` | 44,937 | customer_id, recency_days, segment, bad_orders, neg_tickets, risk_score, risk_level |
| `gold.campaign_priority` | 44,937 | customer_id, segment, Ms, risk_score, priority |
| `gold.category_penetration` | 9 | category, customers, penetration_pct |
| `gold.behavior_funnel` | 4 | step_no, step, customers |
| `gold.cart_abandoners` | 20,652 | customer_id, cart_events |
| `silver.orders` | 182,946 | status, payment_method, order_date, order_amount, product_id |
| `silver.product` | 410 | product_id, category, price |
| `silver.clickstream` | 305,114 | event_type, event_time |
| `metadata.dq_summary` | per run | run_id, source, severity, reason, rows, source_rows, quarantined_rows |
| `metadata.integrity` | per run | run_id, relationship, child_rows, orphans_vs_bronze, orphans_vs_silver |
| `metadata.ingestion` | per run | run_id, source, rows, seconds, ingested_at |

Prefer the Gold tables. Query Silver only where Gold does not cover the metric, and
say so in the chart description. Metadata tables hold one batch per run, so charts
read them through the `_latest` datasets below.

`session_id` and `device_id` in the clickstream are not usable keys (design document
§3.2), so no chart groups by them.

**Datasets.** A table dataset is named after its table, for example `customer_360` in
schema `gold`. Six SQL datasets cover what no single table does:

| Dataset | Content |
|---|---|
| `metadata_dq_summary_latest`, `metadata_integrity_latest`, `metadata_ingestion_latest` | the latest batch of each metadata table |
| `silver_revenue_by_category` | successful orders with their product category, `unknown` where the product was quarantined |
| `silver_clickstream_hourly` | events by weekday and hour from 2025-09-02 |
| `gold_behavior_funnel_dropoff` | funnel steps with the drop-off from the previous step |

Calculated columns: `tenure_years` on `customer_360`, `buyer_type` on `customer_rfm`,
`risk_band` on `customer_risk` and `step_label` on `behavior_funnel`. Trino returns
column names in lower case, so `R`, `Fs` and `Ms` appear as `r`, `fs` and `ms`.

---

## Dashboard 1 — Customer overview

Answers: who are our customers.

| Chart | Type | Source | Fields and computation |
|---|---|---|---|
| Customers | big number | `gold.customer_360` | `count(*)`, expect 48,200 |
| Customers with orders | big number | `gold.customer_360` | `count(*) where segment <> 'No Purchase'`, expect 44,937 |
| Acquisition channel | pie | `gold.customer_360` | group by `source` |
| Gender split | pie | `gold.customer_360` | group by `gender` |
| Countries | bar, top 15 | `gold.customer_360` | group by `country`, descending |
| Customer tenure | bar | `gold.customer_360` | customers by `tenure_years`, whole years since signup |
| Customer lookup | table | `gold.customer_360` | every column, highest spend first; the Customer ID filter narrows it to one customer |

**Done when** both big numbers match the table above and no chart exposes email,
phone or address.

---

## Dashboard 2 — RFM segmentation

Answers: how customer value is distributed.

Recency and monetary are scored into quintiles (`R`, `Ms`) and frequency by order
count capped at 5 (`Fs`); customers with the same value always share a score. The
combination of `R` and `Fs` places each customer in one of five segments.

| Chart | Type | Source | Fields and computation |
|---|---|---|---|
| Customers per segment | bar | `gold.customer_rfm` | group by `segment`, descending |
| Average spend per segment | bar | `gold.customer_rfm` | `avg(monetary)` by `segment` |
| Average frequency per segment | bar | `gold.customer_rfm` | `avg(frequency)` by `segment` |
| Spend distribution | histogram | `gold.customer_rfm` | bucket `monetary` |
| High-value share | big number | `gold.customer_rfm` | share of customers with `Ms = 5`, 20% by construction |
| Customers scored | big number | `gold.customer_rfm` | `count(*)`, expect 44,937 |

Run 5: Hibernating 11,615, Loyal 10,188, Champions 8,581, Others 8,167, At Risk 6,386.

**Done when** the five segments sum to 44,937 and `monetary` contains no NaN.

---

## Dashboard 3 — Churn risk and campaign priority

Answers: who to contact first.

The risk score adds three signals, 0 to 2 points each: time since the last order,
refunded or failed orders, and negative-sentiment support tickets. The total, 0 to 6,
maps to four levels. The charts label each level with its score range (`risk_band`:
`0 Very Low`, `1 Low`, `2-3 Medium`, `4-6 High`), which also keeps them in order.

| Chart | Type | Source | Fields and computation |
|---|---|---|---|
| Risk level distribution | bar | `gold.customer_risk` | group by `risk_band` |
| Segment by risk | stacked bar | `gold.customer_risk` | group by `segment`, stack `risk_band` |
| What drives the score | bar | `gold.customer_risk` | `avg(bad_orders)` and `avg(neg_tickets)` by `risk_band` |
| Priority distribution | bar | `gold.campaign_priority` | group by `priority` |
| High-priority customers | table | `gold.customer_360` | `priority = 'HIGH'` ordered by `monetary`: masked name, segment, risk level, bad orders, negative tickets, spend |

Run 5: risk Medium 28,462, High 9,976, Low 4,759, Very Low 1,740; priority
MEDIUM 28,769, HIGH 14,487, LOW 1,681.

**Done when** the table shows `full_name_masked` and no real names.

---

## Dashboard 4 — Product and category penetration

Answers: which categories sell, and which customers they reach.

| Chart | Type | Source | Fields and computation |
|---|---|---|---|
| Penetration by category | bar | `gold.category_penetration` | `penetration_pct` descending, `unknown` in grey |
| Favourite category | bar | `gold.customer_360` | group by `favourite_category` |
| Revenue by category | bar | `silver_revenue_by_category` | sum `order_amount` by `category`: `silver.orders` left join `silver.product` where `status = 'success'`, null category as `unknown`; no Gold table covers revenue |
| Payment methods | pie | `silver.orders` | group by `payment_method`: card, wallet, upi, cash, unknown |
| Repeat versus one-off | pie | `gold.customer_rfm` | `frequency = 1` against `frequency > 1` |

Run 3 penetration: unknown 42.3%, clothing 30.8%, kitchen 29.6%, beauty 28.3%,
home 26.7%, sports 25.0%, automotive 24.7%, electronics 24.5%, toys 23.0%.

`unknown` holds orders whose product was quarantined (90 of 500 products). It reaches
42.3% of buyers, so it must stay visible: dropping it would hide how far one
rejection in the catalogue spreads. The dashboard gives every category a fixed colour
and keeps grey for `unknown`.

**Done when** exactly eight named categories plus `unknown` appear.

---

## Dashboard 5 — Behaviour and conversion

Answers: where customers drop out between browsing and buying.

Every chart here covers 2025-09-02 to 2025-12-01, the 90 days before the reference
date and the only period in which browsing and purchase data overlap. The funnel is
nested: each step's customer set is a subset of the previous one, so the counts always
decrease and the gap between steps is the loss.

| Chart | Type | Source | Fields and computation |
|---|---|---|---|
| Conversion funnel | funnel | `gold.behavior_funnel` | `customers` per `step_label` (`1. page_view` to `4. purchase`), in step order, with the share of the first step |
| Drop-off per step | bar | `gold_behavior_funnel_dropoff` | difference between consecutive steps over the earlier step |
| Cart abandoners | table | `gold.cart_abandoners` | ordered by `cart_events` descending |
| Activity by hour | heatmap | `silver_clickstream_hourly` | `event_time` from 2025-09-02 on, by weekday and hour |
| Events per customer | histogram | `gold.customer_360` | bucket `events_90d` |

Run 3 funnel: page_view 47,055 → search 28,644 → add_to_cart 17,568 → purchase 5,320,
a drop of 39.1%, 38.7% and 69.7%. 20,652 customers added to cart without ordering.

**Done when** the four funnel bars decrease left to right and the cart abandoners
table has 20,652 rows.

---

## Dashboard 6 — Data quality

Answers: what the cleaning layer rejected, what it flagged, and what it cost downstream.

Every chart shows the latest run, through the `_latest` datasets.

| Chart | Type | Source | Fields and computation |
|---|---|---|---|
| Rejection rate per source | bar | `metadata_dq_summary_latest` | `max(quarantined_rows) / max(source_rows)` per `source`. Do not sum the reject `rows`: a row can fail several rules, so they add up to more than the rows quarantined |
| Rejection reasons | stacked bar | `metadata_dq_summary_latest` | `severity = 'reject'`, group by `source`, stack `reason` |
| Warnings | stacked bar | `metadata_dq_summary_latest` | `severity = 'warn'`, group by `source`, stack `reason` |
| Orphans before and after cleaning | grouped bar | `metadata_integrity_latest` | `orphans_vs_bronze` beside `orphans_vs_silver` per `relationship` |
| Ingestion time | bar | `metadata_ingestion_latest` | `seconds` by `source` |

Run 3 rejection rates: orders 39.0%, clickstream 39.0%, tickets 30.1%, product 18.0%,
CRM 3.6% (all of it duplicate customer records). Largest warnings: missing quantity
27,521, non-positive quantity 18,130, order before signup 14,922.

The orphan chart is the one to lead with: every relationship has 0 orphans against
Bronze, but orders → product has 33,022 against Silver. The source data is intact;
the orphans are what our own quarantine of 90 products does to the orders.

**Done when** the rejection rates match the ones above.

---

## Customer drill-down

A single customer's record: masked name, geography, acquisition channel, tenure, the
three RFM values and segment, bad orders, favourite category, ticket counts and last
issue, events and cart events in the last 90 days, risk level and score, and campaign
priority. Two ways to open it:

- **Drill to detail.** Right-click a chart and choose *Drill to detail*, or *Drill to
  detail by* a value, to list the rows behind it. On charts that read
  `gold.customer_360` these are the full customer records; other charts list the rows
  of their own dataset.
- **Customer lookup**, at the bottom of Dashboard 1: every customer, highest spend
  first. The Customer ID filter in the filter bar narrows it to one customer.

**Masked fields only.** `gold.customer_360` excludes email, phone and address at the
point it is built, so no dashboard misconfiguration can leak them. State this in the
report.

---

## Changing a dashboard

Superset stores dashboards in its own database, not in Iceberg. After changing one in
the UI, write it back to the repository and commit `infra/superset/dashboards/`:

```bash
cd infra
docker compose exec superset bash /app/assets.sh export
```

A fresh deployment imports the committed version on its first start. The commands for
re-importing and for starting from an empty Superset are in `infra/README.md`.

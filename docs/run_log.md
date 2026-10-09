# Pipeline run log

One entry per pipeline run: what was planned, what was executed, what came out.
Numbers recorded here feed the report.

---

## Run 1 — 2026-09-25 — first execution on Iceberg

**Goal.** Design document §9 step 2. Produce the Bronze, Silver, Quarantine and Gold
tables in MinIO, verify the Spark 3.5.5 compatibility fixes, and collect the data
quality numbers for the report.

**Environment.** WSL2 Ubuntu 24.04, Docker Engine 29.8.1, Compose v5.5.1,
`tabulario/spark-iceberg:3.5.5_1.8.1` (Spark 3.5.5, Iceberg 1.8.1),
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`, pyspark 3.5.5.

**Plan.**

1. `docker compose up -d`, wait for the REST catalog to report healthy.
2. Run the pipeline inside the Spark container in iceberg mode, tee the output to
   `docs/pipeline_run.log` from the host so the file stays owned by the normal user.
3. Verify: five namespaces under `demo`, seven tables under `demo.gold`, data files
   present in the MinIO `warehouse` bucket.
4. Extract the rejection rates, layer row counts and ingestion durations.

**Commands.**

```bash
cd ~/customer360-lakehouse/infra
docker compose up -d
docker compose exec -T spark-iceberg bash -lc '
  cd /home/iceberg/project &&
  export LAKEHOUSE_MODE=iceberg LOCAL_DATA=/home/iceberg/data &&
  ./run_pipeline.sh' 2>&1 | tee ~/customer360-lakehouse/docs/pipeline_run.log
```

**Watch items.** The `clickstream` column named `timestamp` shares its name with the
SQL type keyword; the Spark driver runs with the default 1 GB heap; `multiLine=True`
makes the CSV reader single-threaded per file.

**Result.** Succeeded end to end in 2 min 42 s (16:44:12 to 16:46:54). Exit code 0.
The Spark 3.5.5 date fixes work: no parser errors, and the rejection counts are the
ones the code was written against. Full console output in `pipeline_run.log`.

Ingestion, 880,500 rows in total. The 11.88 s for orders includes Spark start-up.

| Source | Rows | Seconds |
|---|---|---|
| orders | 300,000 | 11.88 |
| clickstream | 500,000 | 5.96 |
| crm | 50,000 | 2.07 |
| tickets | 30,000 | 1.07 |
| product | 500 | 0.66 |

Data quality. 295,347 of 880,500 rows quarantined, 33.5 % overall.

| Source | Valid | Quarantined | Rate | Reasons |
|---|---|---|---|---|
| orders | 191,880 | 108,120 | 36.0 % | bad_order_date 90,000; nonpositive_order_amount 25,798 |
| clickstream | 321,343 | 178,657 | 35.7 % | missing_customer_id 150,553; bad_event_time 40,171 |
| tickets | 21,498 | 8,502 | 28.3 % | bad_created 7,179; resolved_before_created 1,323 |
| product | 432 | 68 | 13.6 % | nonpositive_price 45; missing_category 30 |
| crm | 50,000 | 0 | 0.0 % | none; CRM defects are normalised, not rejected |

Gold, 45,288 customers with at least one successful order, out of 50,000 in CRM.

| Table | Content |
|---|---|
| customer_rfm | Champions 11,222, Hibernating 11,089, Loyal 8,923, Others 7,027, At Risk 7,027 |
| customer_risk | Medium 28,320, High 10,461, Low 4,782, Very Low 1,725 |
| campaign_priority | MEDIUM 28,808, HIGH 14,748, LOW 1,732 |
| category_penetration | clothing 31.0 %, kitchen 30.8 %, beauty 28.7 %, sports 26.3 %, home 26.0 % |
| behavior_funnel | login 23,202, search 30,222, page_view 47,260, add_to_cart 30,324 |
| cart_abandoners | 1,820 customers |
| customer_360 | 50,000 rows, one per CRM customer |

Storage: 100 objects, 98 MiB in the MinIO `warehouse` bucket. Five namespaces under
`demo`, seven tables under `demo.gold`. `silver.customer_device` holds 53,796 rows
from exploding the multi-valued device field.

**Three defects the run exposed.**

1. `order_amount` accepts NaN. `try_cast` turns the literal text into a NaN double,
   which is neither null nor `<= 0`, so both the parse check and the positivity check
   pass it. 8,934 rows reach `silver.orders` with a NaN amount and 6,189 of the 45,288
   customers end up with a NaN `monetary`, which also corrupts their M quintile and
   therefore their campaign priority. Fix: add an `isnan` condition to
   `rule_positive`, or add a dedicated NaN rule.
2. `silver.product` still has 23 distinct categories where the catalogue has 8. The
   cleaning lowercases, trims and repairs `3` to `e`, but leaves truncations (`clo`,
   `hom`, `kit`) and suffixes (`clothing-`, `home_`). Category penetration therefore
   splits one category across several rows. Fix: map to a canonical set by prefix.
3. `behavior_funnel` is not monotonic: page_view 47,260 exceeds add_to_cart 30,324 and
   search 30,222, both of which exceed login 23,202. The declared step order
   login, search, page_view, add_to_cart does not describe this data, so a funnel
   chart drawn from it will look broken. Fix: either order the steps by observed
   customer count or define the funnel over session sequences rather than distinct
   customers per event type.

**Out of scope.** Local-mode cross-check (step 3), Trino (step 4), Superset (step 5).

---

## Run 2 — 2026-09-25 — after fixing the three defects from Run 1

**Goal.** Fix the NaN amounts, the unnormalised categories and the non-monotonic
funnel that Run 1 exposed, then re-run the whole pipeline.

**Changes.**

| File | Change |
|---|---|
| `src/dq/framework.py` | new `rule_not_nan`: `try_cast` yields NaN for some text, and NaN is neither null nor `<= 0`, so the existing rules passed it |
| `src/silver_clean.py` | `rule_not_nan` added to `order_amount` and `price`; new `_canonical_category()` maps truncated, suffixed and leetspeak values onto the eight catalogue categories, anything else becomes null and is quarantined |
| `src/gold_metrics.py` | `build_funnel` rebuilt as a nested funnel over intersecting customer sets, with `purchase` from `silver.orders` as the final step |

**Result.** Succeeded in 2 min 34 s (17:20:01 to 17:22:35). Exit code 0.

| Check | Run 1 | Run 2 |
|---|---|---|
| NaN `order_amount` reaching Silver | 8,934 | 0 |
| NaN `monetary` in `gold.customer_rfm` | 6,189 | 0 |
| Distinct categories in `silver.product` | 23 | 8 |
| Funnel monotonic | no | yes |

Data quality after the fix. Two new reason codes appear, `nan_order_amount` and
`nan_price`, so the rejected share rises where the old rules were letting bad rows
through.

| Source | Valid | Quarantined | Rate | Reasons |
|---|---|---|---|---|
| orders | 182,946 | 117,054 | 39.0 % | bad_order_date 90,000; nonpositive_order_amount 25,798; nan_order_amount 12,830 |
| clickstream | 321,343 | 178,657 | 35.7 % | missing_customer_id 150,553; bad_event_time 40,171 |
| tickets | 21,498 | 8,502 | 28.3 % | bad_created 7,179; resolved_before_created 1,323 |
| product | 410 | 90 | 18.0 % | nonpositive_price 45; missing_category 30; nan_price 26 |
| crm | 50,000 | 0 | 0.0 % | none |

Gold, 44,937 customers with at least one successful order.

| Table | Content |
|---|---|
| customer_rfm | Champions 11,858, Hibernating 11,603, Loyal 8,730, Others 6,373, At Risk 6,373 |
| customer_risk | Medium 28,429, High 10,025, Low 4,750, Very Low 1,733 |
| campaign_priority | MEDIUM 28,763, HIGH 14,496, LOW 1,678 |
| category_penetration | clothing 30.8 %, kitchen 29.6 %, beauty 28.3 %, home 26.7 %, sports 25.0 %, automotive 24.7 %, electronics 24.5 %, toys 23.0 % |
| behavior_funnel | page_view 47,260 → search 29,643 → add_to_cart 18,745 → purchase 17,538 |
| cart_abandoners | 2,032 customers |
| customer_360 | 50,000 rows |

Funnel drop-off: 37.3 % from page_view to search, 36.8 % from search to add_to_cart,
6.4 % from add_to_cart to purchase.

Ingestion, unchanged from Run 1 within noise: orders 9.57 s, clickstream 6.67 s,
crm 1.67 s, tickets 1.08 s, product 0.65 s.

**Next.** Design document §9 step 3, the local-mode cross-check, then Trino and
Superset.

---

## Run 3 — 2026-09-26 — Customer 360 completeness

**Goal.** Close the gaps an audit of the data found against what a Customer 360 needs:
one row per customer, all five sources integrated into that row, and a common time
frame.

**Audit findings.**

| Area | Finding |
|---|---|
| Grain | `customer_360` had 50,000 rows for 48,200 ids; 1,734 ids are duplicated, 481 of them with conflicting content |
| Integration | clickstream covers 99.9% of customers and tickets 36%, yet `customer_360` carried no behaviour or support column |
| Time | no reference date; orders end 2025-12-01 while clickstream runs 2025-09-02 to 2028-02; 13,450 events and 457 tickets fall after the last order |
| Validity | quantity invalid in 45,651 orders (25%); about 14,900 orders and 1,711 tickets precede the customer's signup; 1,366 tickets disagree with their own resolution time; 33,022 orders vanished in an inner join after their product was quarantined |

Verified and not acted on: emails and phones shared across ids belong to different
people (for example five unrelated customers under `shared1459@hotmail.com`), so ids
are not merged; source foreign keys have no orphans; clickstream device ids never
appear in the CRM.

**Changes.**

| File | Change |
|---|---|
| `spark_session.py` | `AS_OF_DATE` 2025-12-01, `BEHAVIOUR_WINDOW_DAYS` 90, shared `RUN_ID` |
| `run_pipeline.sh` | a fresh `RUN_ID` per run |
| `dq/framework.py` | warning severity, `rule_after`, `rule_matches`, `dedup`, persistable `dq_summary` |
| `silver_clean.py` | CRM golden record per customer; primary-key dedup on every source; reference-date rejection; cross-source and quantity warnings; `metadata.dq_summary` and `metadata.integrity` |
| `gold_metrics.py` | recency from `AS_OF_DATE`; funnel and cart abandoners inside the 90-day window; `unknown` category bucket; `customer_360` with tenure, purchases, support and behaviour columns |

**Decisions.** Orders and tickets dated before signup are warnings, since a guest order
can precede account creation. Invalid quantity is a warning and is set to null; no
metric uses it.

**Plan.** Compile, run the pipeline, then assert: `customer_360` rows equal distinct
ids equal `silver.crm` rows; nothing in Silver is dated after 2025-12-01; Silver plus
quarantine equals Bronze for every source; the funnel decreases; the golden record for
the `Parrish` / `Ppaarrrish` id keeps `Parrish`.

**Result.** The first attempt, `run_20260926T163133Z`, stopped after Silver had
written its tables and `metadata.dq_summary`: `explode` cannot be nested inside `trim`
in `_devices()`. Its summary rows remain in `metadata.dq_summary` under that run_id.
After the fix, `run_20260926T163523Z` succeeded in 4 min 18 s (18:35:22 to 18:39:40).

| Source | Valid | Quarantined | Rate | Rejected | Warned |
|---|---|---|---|---|---|
| orders | 182,946 | 117,054 | 39.0% | bad_order_date 90,000; nonpositive_order_amount 25,798; nan_order_amount 12,830 | missing_quantity 27,521; nonpositive_quantity 18,130; order_before_signup 14,922; unknown_payment_method 13,016 |
| clickstream | 305,114 | 194,886 | 39.0% | missing_customer_id 150,553; bad_event_time 40,171; after_as_of_event_time 23,189 | — |
| tickets | 20,976 | 9,024 | 30.1% | bad_created 7,179; after_as_of_created 1,845; resolved_before_created 1,323 | ticket_before_signup 1,711; resolved_after_as_of 1,414; resolution_time_mismatch 1,310 |
| product | 410 | 90 | 18.0% | nonpositive_price 45; missing_category 30; nan_price 26 | — |
| crm | 48,200 | 1,800 | 3.6% | duplicate_customer_id 1,800 | corrupted_last_name 1,491; corrupted_first_name 1,464 |

557,646 valid and 322,854 quarantined out of 880,500 rows, 36.7% overall.

Integrity (`metadata.integrity`):

| Relationship | Rows | Orphans vs Bronze | Orphans vs Silver |
|---|---|---|---|
| orders.product_id → product | 182,946 | 0 | 33,022 |
| orders.customer_id → crm | 182,946 | 0 | 0 |
| tickets.customer_id → crm | 20,976 | 0 | 0 |
| clickstream.customer_id → crm | 305,114 | 0 | 0 |
| clickstream.device_id → crm.device_ids | 305,114 | 305,114 | 305,114 |

Gold:

| Table | Run 2 | Run 3 |
|---|---|---|
| customer_360 | 50,000 rows for 48,200 ids | 48,200 rows, one per customer |
| behavior_funnel | 47,260 → 29,643 → 18,745 → 17,538, all history | 47,055 → 28,644 → 17,568 → 5,320, 90-day window |
| cart_abandoners | 2,032, never ordered at all | 20,652, no order within the window |
| category_penetration | 8 categories | 8 categories plus unknown at 42.3% |
| customer_rfm | 44,937 customers | 44,937 customers, recency from AS_OF_DATE |

In the 90-day window, 30.3% of customers who went from page view through search to
cart also ordered. The Run 2 figure of 93.6% compared three months of browsing with
two years of orders.

`customer_360` coverage: 44,937 customers with purchases, 16,877 with tickets, 48,104
(99.8%) with behaviour in the window, 42,959 with a favourite category; mean tenure
2,191 days.

Verification: 13 of 13 checks passed, including one row per customer, `Parrish`
kept over `Ppaarrrish` for id `e04c1a04-…`, nothing in Silver after 2025-12-01, and
Silver plus quarantine equal to Bronze for every source.

**New finding.** `session_id` does not identify a session: 305,114 events carry 8,000
distinct values, each shared by 38.1 customers on average and at most 62, and none
belongs to a single customer. The `sessions_90d` column added in this run was
therefore meaningless, equal to `events_90d` for almost every customer, and was
removed with a Gold-only rerun; `customer_360` has 23 columns.

Ingestion: orders 9.45 s, clickstream 4.80 s, crm 1.69 s, tickets 1.13 s,
product 0.70 s.

**Next.** Design document §9 step 3, the local-mode cross-check, then Trino and
Superset.

---

## Run 4 — 2026-09-30 — local versus Iceberg cross-check

**Goal.** Design document §9 step 3: run the same code once against Iceberg and once
against local Parquet and require identical results, which shows that the storage
abstraction in `lakehouse_io.py` does not change any outcome.

**Found before running.** The RFM quintiles depended on row order. `frequency` has 15
distinct values across 44,937 customers, so tie groups straddle every quintile
boundary, and `NTILE` placed tied customers on either side according to the order in
which rows arrived. Rebuilding `customer_rfm` from the same Silver rows after
`repartition(16)` changed `Fs` for 3,028 customers, `R` for 30 and the segment for 16.
This also explains the one-customer segment differences between Run 2 and Run 3. A
cross-check would have reported these as mismatches although the storage layer was
not involved.

**Changes.**

| File | Change |
|---|---|
| `gold_metrics.py` | `customer_id` as the second sort key in the three `NTILE` windows; `last_issue_type` taken from `max(struct(created, ticket_id, issue_type))` instead of `max_by`, which is also arbitrary on ties |

After the change the repartitioned rebuild differs for 0 customers.

**Plan.** Run the pipeline in iceberg mode, then in local mode with
`LOCAL_OUT=/tmp/c360_local` inside the Spark container. Compare all 23 tables:
equal row counts; `exceptAll` empty in both directions on every non-floating column,
ignoring the audit columns `_ingest_ts` and `_ingest_run_id`; floating columns in Gold
within 0.01 by key, since distributed sums add in no fixed order. Compare the latest
`metadata.dq_summary` and `metadata.integrity` batches without `run_id`, and
`metadata.ingestion` on row counts only.

**Result.** All 26 comparisons identical: 23 tables and 3 metadata batches, with equal
row counts and empty `exceptAll` in both directions. The largest floating-point
difference in `monetary` is 0.0. The storage abstraction changes no outcome.

| Run | Mode | Start | Duration |
|---|---|---|---|
| 4a `run_20260930T214409Z` | iceberg | 23:44:09 | 4 min 32 s |
| 4b | local Parquet in `/tmp/c360_local` | 23:48:41 | 10 min 15 s |

Silver, quarantine, integrity, risk and priority are the same as Run 3. RFM segments
moved:

| Segment | Run 3 | Run 4 |
|---|---|---|
| Champions | 11,858 | 10,017 |
| Hibernating | 11,604 | 10,690 |
| Loyal | 8,731 | 9,658 |
| Others | 6,372 | 7,286 |
| At Risk | 6,372 | 7,286 |

**Finding.** Reproducible is not the same as fair. With `customer_id` breaking ties,
the scores repeat on every run, but customers with identical behaviour still receive
different scores:

| Frequency | Customers | Fs scores given |
|---|---|---|
| 2 orders | 11,451 | 1, 2 and 3 |
| 3 orders | 10,428 | 3 and 4 |
| 4 orders | 7,264 | 4 and 5 |

29,143 customers, 65% of buyers, sit in a frequency group that `NTILE(5)` splits
across quintiles; for recency the same happens to 414 customers. The cause is the
method: `NTILE` cuts the customer list into five equal counts, and `frequency` has 15
values concentrated between 1 and 4, so equal counts cannot fall on value boundaries.
Which tie-break is used then decides segment membership for about 1,800 customers,
which is the Run 3 to Run 4 shift above.

Frequency distribution over the 44,937 buyers: 1 order 18.5%, 2 orders 25.5%,
3 orders 23.2%, 4 orders 16.2%, 5 or more 16.6%.

**Next.** Replace `NTILE` with tie-aware scoring, then design document §9 step 4.

---

## Run 5 — 2026-10-01 — tie-aware RFM scoring

**Goal.** Give customers with identical behaviour identical RFM scores.

**Change.** In `build_rfm`, `Fs` is the order count capped at 5, so 1, 2, 3, 4 and 5+
orders score 1 to 5 and hold 18.5, 25.5, 23.2, 16.2 and 16.6% of buyers. `R` and `Ms`
are quintiles from `percent_rank`, which gives tied values the same rank and therefore
the same score. No tie-break key is needed.

**Plan.** Silver is unchanged, so only Gold is rerun, once in iceberg mode and once in
local mode on the Run 4 Silver in `/tmp/c360_local`. Check that no recency, frequency
or monetary value maps to more than one score, then repeat the 26-way cross-check.

**Result.** Gold rerun in both modes (00:10:11 to 00:12:01). No recency, frequency or
monetary value maps to more than one score, and the 26-way cross-check is still
identical, 26 of 26.

Score distribution over the 44,937 buyers:

| Score | R | Fs | Ms |
|---|---|---|---|
| 1 | 20.1% | 18.5% | 20.0% |
| 2 | 20.0% | 25.5% | 20.0% |
| 3 | 20.1% | 23.2% | 20.0% |
| 4 | 19.9% | 16.2% | 20.0% |
| 5 | 20.0% | 16.6% | 20.0% |

| Segment | Run 4 | Run 5 |
|---|---|---|
| Hibernating | 10,690 | 11,615 |
| Loyal | 9,658 | 10,188 |
| Champions | 10,017 | 8,581 |
| Others | 7,286 | 8,167 |
| At Risk | 7,286 | 6,386 |

Others and At Risk are no longer equal. They were equal in Runs 2, 3 and 4 only
because `NTILE` forced equal counts per quintile.

Campaign priority moved by one customer, MEDIUM 28,769 and HIGH 14,487, from a
monetary tie on a quintile boundary. Risk levels are unchanged: Medium 28,462,
High 9,976, Low 4,759, Very Low 1,740.

**Next.** Design document §9 step 4, Trino.

---

## Run 6 — 2026-10-01 — quarantined rows per source

**Goal.** Record how many rows each source sends to quarantine, so that Dashboard 6
can show a rejection rate without reading the quarantine tables, which Trino hides for
CRM.

**Found in step 4.** Read through Trino, the reject counts in `metadata.dq_summary`
add up to more than the rows actually quarantined, because a row can fail several
rules:

| Source | Sum of reject counts | Rows in `quarantine.<source>` |
|---|---|---|
| clickstream | 213,913 | 194,886 |
| orders | 128,628 | 117,054 |
| tickets | 10,347 | 9,024 |
| product | 101 | 90 |
| crm | 1,800 | 1,800 |

The rejection rate in the dashboard blueprint, the sum of reject `rows` over
`source_rows`, would have shown orders at 42.9% instead of 39.0%.

**Change.** `dq/framework.py`: every `dq_summary` row carries `quarantined_rows` next
to `source_rows`. The rejection rate of a source is `quarantined_rows / source_rows`.

**Plan.** Add the column to the existing `demo.metadata.dq_summary` with
`ALTER TABLE ... ADD COLUMN`, so earlier batches keep their rows with a null value. Run
the pipeline in iceberg mode, then in local mode into an empty `/tmp/c360_local`, and
repeat the 26-way cross-check. Assert that `quarantined_rows` equals the row count of
`quarantine.<source>` for all five sources, and that the DQ totals, segments, risk
levels and priorities are the same as in Run 5. Then read the rates through Trino.

**Result.** Succeeded in both modes. `quarantined_rows` equals the quarantine table
row count for all five sources, and the 26-way cross-check is identical, 26 of 26,
including the new column.

| Run | Mode | Start | Duration |
|---|---|---|---|
| 6a `run_20260930T224132Z` | iceberg | 00:41:31 | 4 min 57 s |
| 6b | local Parquet in `/tmp/c360_local` | 00:46:28 | 4 min 17 s |

| Source | `quarantined_rows` | Sum of reject counts | Source rows | Rejection rate |
|---|---|---|---|---|
| clickstream | 194,886 | 213,913 | 500,000 | 39.0% |
| orders | 117,054 | 128,628 | 300,000 | 39.0% |
| tickets | 9,024 | 10,347 | 30,000 | 30.1% |
| product | 90 | 101 | 500 | 18.0% |
| crm | 1,800 | 1,800 | 50,000 | 3.6% |

The rates read through Trino are the same. The four earlier batches in
`metadata.dq_summary` keep their 22 rows each, with `quarantined_rows` null.

Everything else equals Run 5: the DQ totals, `customer_360` with 48,200 rows for
48,200 customers, the segments, risk levels, priorities and the `integrity` batch.

**Next.** Design document §9 step 5, Superset.

---

## Run 7 — 2026-10-08 — churn classifier

**Goal.** Design document §9 step 6: train a churn classifier on purchase, support and
profile history, evaluate it on held-out customers next to the rule-based risk score
of Gold, and score every customer.

**Definition.** The cutoff is 2025-09-02, 90 days before the reference date. The
population is every customer with a successful order before the cutoff. A customer is
churned if they place no successful order from the cutoff to 2025-12-01. Features use
only data dated before the cutoff, and the program stops if an order on or after the
cutoff reaches them.

**Found before running.** The label barely depends on purchase history. Of 43,683
customers in the population, 30,787 (70.5%) churn, and the rate hardly moves with what
a churn model would use:

| Successful orders before the cutoff | Customers | Churned |
|---|---|---|
| 1 | 10,210 | 71.5% |
| 2 | 12,417 | 70.8% |
| 3 | 9,814 | 70.8% |
| 4 | 6,147 | 70.3% |
| 5 | 3,009 | 68.9% |
| 6 | 1,254 | 66.8% |
| 7 | 525 | 64.4% |
| 8 or more | 307 | 53.7% |

| Days since the last order, at the cutoff | Customers | Churned |
|---|---|---|
| 0–90 | 14,031 | 69.8% |
| 91–180 | 9,826 | 70.2% |
| 181–365 | 12,184 | 71.5% |
| over 365 | 7,642 | 70.5% |

Successful orders are spread evenly over time, about 16,800 per quarter. The figures
fit orders that are assigned to customers at random, in which case past behaviour
cannot say who orders next. A trial of the model stage on the Run 6 Silver tables in
local mode gave a held-out AUC of 0.496 for the rule-based score and 0.512 and 0.518
for the two models.

**Changes.**

| File | Change |
|---|---|
| `src/ml/churn_model.py` | new: features at a reference date, label, hash-based test split (one customer in five), logistic regression and random forest tuned by 3-fold cross-validation, the Gold risk score recomputed at the cutoff as a baseline, scores for every customer |
| `gold_metrics.py` | `build_rfm` takes an optional `as_of`, so the baseline can be computed at the cutoff; the default is unchanged |
| `run_pipeline.sh` | stage 4, the churn model |
| `requirements.txt` | `numpy`, which `pyspark.ml` needs |

New tables: `gold.churn_scores`, overwritten per run, and `metadata.ml_metrics`,
appended per run with one row per model and one for the rule-based score.

**Plan.** Run the pipeline in iceberg mode, then in local mode into an empty
`/tmp/c360_local`. Check that Gold equals Run 6, that the test and training customers
do not overlap, that `churn_scores` has 44,937 rows with probabilities in [0, 1], and
that the metrics equal the local trial. Extend the cross-check with `churn_scores` and
the latest `ml_metrics` batch and require all 28 comparisons to match. Record the time
of stage 4.

**Result.** Succeeded after two fixes that the cross-check exposed. Neither the
rule-based risk score nor the models predict churn on this data.

| Pass | Run | What ran | Cross-check |
|---|---|---|---|
| 1 | `run_20261008T195210Z` and local | full pipeline, both modes | 27 of 28: `ml_metrics` differs by up to 3.1e-6 |
| 2 | `run_20261008T201026Z` and local | full pipeline, money features rounded to cents | 27 of 28: up to 3.7e-6 |
| 3 | `run_20261008T202746Z` and local | stage 4 only, exact AUC curves | 28 of 28; largest difference 1.0e-15 |
| 4 | `run_20261008T203554Z` | full pipeline, iceberg, final code; `docs/pipeline_run.log` | metrics equal pass 3 |

The scores in `churn_scores` were identical in every pass. The two fixes:
- **Money features.** Unrounded, `monetary` differed between the modes by up to 4.7e-10 for 4,132 customers, from the order in which partial sums are added. Rounding to cents, as Gold does, removes it.
- **AUC curves.** `BinaryClassificationEvaluator` groups the sorted scores into 1,000 bins by default, and the grouping depends on how Spark samples them. `numBins=0` computes the exact curve.

The batches of passes 1 and 2 remain in `metadata.ml_metrics` under their run_ids.

| Run | Mode | Start | Total | Stage 4 |
|---|---|---|---|---|
| 7, pass 4 | iceberg | 22:35:54 | 7 min 26 s | 2 min 48 s |
| 7, pass 2 | local | 22:17:38 | 6 min 58 s | 2 min 45 s |

Population 43,683, of whom 30,787 (70.5%) churned. Training set 34,980 customers,
test set 8,703, with 70.1% churned.

| Score | Test AUC-ROC | Test AUC-PR | CV AUC | Chosen parameters |
|---|---|---|---|---|
| rule-based risk score at the cutoff | 0.4964 | 0.7015 | — | — |
| logistic regression | 0.5124 | 0.7061 | 0.5155 | `regParam` 0.1 |
| random forest | 0.5182 | 0.7046 | 0.5185 | 100 trees, depth 5 |

With 6,098 churned and 2,605 retained test customers, the standard error of an AUC near
0.5 is about 0.007 (Hanley–McNeil). The rule-based score is therefore at chance, and
the random forest, the better model, is 0.018 above it. AUC-PR is close to the 70.1%
base rate for all three.

The random forest relies most on distinct categories (importance 0.151), frequency
(0.135) and monetary (0.123). In the logistic regression the largest standardised
coefficient is frequency, −0.030: more orders, slightly less churn.

`gold.churn_scores` holds the random forest's probability, the model with the higher
cross-validated AUC, for all 44,937 customers with a successful order: 0.45 to 0.74,
mean 0.70. Averaged by rule-based risk level it is flat: 69.5% for Very Low, 69.6%
for Low, 70.3% for Medium and 70.4% for High.

Silver and Gold equal Run 6: DQ totals, `customer_360` with 48,200 rows, segments,
risk levels and priorities.

Superset: Dashboard 3 gained a row with *Model versus rule (AUC)* and *Predicted churn
by risk level*, built on two new SQL datasets. The bundle in `infra/superset/dashboards/`
was re-exported from an instance freshly imported from the repository. Besides
Dashboard 3 and the four new files, the export changes:
- the instance-specific dataset ids in the chart parameters
- the `annotation_layers: []` default the importer adds
- the mode of the YAML files, from 755 to 644

**Finding.** On this dataset the risk levels and campaign priorities of Dashboard 3
describe customers but do not predict who leaves, and no model trained on the same
history does much better. The cause is in the data, as the tables before the run
show: past purchases barely change the chance of ordering again.

**Next.** Design document §9 step 7.

---

## Run 8 — 2026-10-09 — reproduction from a fresh clone

**Goal.** Design document §9 step 7: show that a fresh clone, the source CSVs and the
commands in `README.md` reproduce every number of the report, with nothing carried over
from earlier runs.

**Found before running.** Nothing in `README.md` or `data/README.md` said where to get
the CSVs; only the design document named the Kaggle page. `data/README.md` now gives
the link and a command to extract `archive.zip`, and `README.md` points to it.

**Plan.** Clone commit `cdf1c64` into `~/c360-fresh`. The GitHub repository is private,
so the clone is taken from the local repository, which is at the same commit as
`origin/main`. Extract the Kaggle `archive.zip` into `data/` as `data/README.md`
describes. Stop the existing stack with `docker compose down`, which keeps its volumes,
because the containers have fixed names. Start the clone under the compose project name
`c360fresh`, so that every volume is new and empty, then follow `README.md` section B
step by step.

Compare with Run 7:
- per-source quarantine counts
- `customer_360` rows
- segments, risk levels and priorities
- category penetration, the funnel and cart abandoners
- the `orders → product` orphans
- the three AUCs to six decimals
- `churn_scores` rows

Screenshot the six dashboards that Superset imports on its first start. Then remove the
fresh stack with its volumes, restart the original one, and check that its data is
intact.

**Result.** A fresh clone reproduces every number. All 13 checks pass, 47 values in
total, exactly as in Run 7:
- the quarantined and total rows of each source
- `customer_360` with 48,200 rows and 48,200 ids
- segments, risk levels and priorities
- the nine category penetration rates
- the funnel and the 20,652 cart abandoners
- 0 and 33,022 `orders → product` orphans against Bronze and Silver
- the three AUC-ROC and AUC-PR values to six decimals
- 44,937 churn scores

| Step | Time |
|---|---|
| `docker compose up -d --build` until Trino answers | 79 s |
| pipeline `run_20261008T233213Z`, 01:32:13 to 01:40:27 | 8 min 14 s |
| Bronze / Silver / Gold / churn model | 53 s / 3 min 15 s / 1 min 14 s / 2 min 52 s |

The run was about 50 s slower than Run 7, pass 4. The containers were cold, and Superset was starting at the same time.

The new catalog was empty before the pipeline ran: `SHOW SCHEMAS` listed only
`information_schema` and `system`. Superset imported the six dashboards on its first
start, before any table existed, and stayed healthy. After the pipeline all six rendered
without errors in the screenshots, with the Run 7 figures.

Afterwards `docker compose down -v` removed the `c360fresh` volumes, and the original
stack restarted on its own volumes. The same 13 checks passed on it, `metadata.ml_metrics`
still held its four batches, and the dashboards still rendered. The Spark container had
written `__pycache__` files into the clone as root, so the clone had to be deleted as root.

**Next.** The report, `docs/report.docx` and `docs/report.pdf`.

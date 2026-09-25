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

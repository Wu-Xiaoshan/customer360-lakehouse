# data

Source CSVs. Not version-controlled.

Download `archive.zip` (74 MB, Kaggle account required) from
https://www.kaggle.com/datasets/vinaykandimalla/customer-360 and extract the five CSVs
into this directory, from the repository root:

```bash
python3 -m zipfile -e archive.zip data/
```

| File | Size | Content |
|---|---|---|
| `orders_300k_dirty.csv` | 34 MB | 300k orders |
| `clickstream_500k_events.csv` | 122 MB | 500k behavioural events |
| `crm_50000_customers_dirty_v3.csv` | 11 MB | 50k customer records |
| `support_tickets_30000_dirty.csv` | 4 MB | 30k support tickets |
| `product_catalog_dirty_30pct.csv` | <1 MB | product catalogue |

Filenames must match `SOURCES` in [src/spark_session.py](../src/spark_session.py).

Mounted read-only at `/home/iceberg/data` by `infra/docker-compose.yml`.

# infra

Based on the Apache Iceberg quickstart
([tabular-io/docker-spark-iceberg](https://github.com/tabular-io/docker-spark-iceberg),
Apache-2.0). `docker-compose.yml` is self-contained; no external repository is needed.

## Modifications to the upstream file

Also recorded in the header comment of `docker-compose.yml`, as Apache-2.0 requires.

| Change | Reason |
|---|---|
| removed `build: spark/` | the upstream `spark/` build context is not vendored here, so the published image is pulled instead |
| `./notebooks` → `../notebooks` | upstream has no `notebooks/` directory at its root |
| added `..` and `../data:ro` mounts | project code and source CSVs inside the container |
| `minio/minio`, `minio/mc` → pinned `quay.io/minio/*` | both repositories were removed from Docker Hub |
| `tabulario/spark-iceberg` → `tabulario/spark-iceberg:3.5.5_1.8.1` | Spark 3.5.5 / Iceberg 1.8.1; `requirements.txt` pins the same PySpark version |
| removed `mc rm -r --force minio/warehouse`; named volumes `minio_data` and `rest_catalog`; `CATALOG_URI` on a SQLite file | upstream empties the warehouse and keeps the catalog in memory, so every table was lost on restart |
| added `trino` (`trinodb/trino:483`) and `./trino` | SQL layer for Superset; see [Trino](#trino) |
| added `superset`, built from `apache/superset:6.1.0` in `./superset`, and the `superset_home` volume | dashboards; see [Superset](#superset) |

Mount paths are relative to `infra/`:

```
customer360-lakehouse/   ← ..              /home/iceberg/project
├── infra/               ← .               compose file
├── notebooks/           ← ../notebooks
└── data/                ← ../data         read-only
```

Each service added in a later step needs a matching line in that header comment.

## Startup

```bash
cd infra
docker compose config      # four mounts expected on spark-iceberg
docker compose up -d       # builds the superset image on the first run
docker compose ps          # six services running
```

Running the pipeline inside the container:

```bash
docker compose exec spark-iceberg bash
cd /home/iceberg/project
ls data/                   # five CSVs expected
./run_pipeline.sh
```

Docker creates `data/` if it is absent, so an empty `ls data/` means the CSVs have
not been copied. See [data/README.md](../data/README.md).

## Services and ports

Six services on the `iceberg_net` network.

| Service | Port | Purpose |
|---|---|---|
| spark-iceberg | 8888 | Jupyter |
| spark-iceberg | 8080 | Spark UI |
| spark-iceberg | 10000, 10001 | Thrift server |
| rest | 8181 | Iceberg REST catalog |
| minio | 9000 | S3 API |
| minio | 9001 | MinIO console |
| mc | none | creates the `warehouse` bucket |
| trino | 8085 (8080 in the container) | Trino, SQL over the Iceberg tables |
| superset | 8088 | Superset dashboards, login `admin` / `admin` |

Check for conflicts before starting, particularly on 8080:

```bash
ss -tlnp | grep -E '8888|8080|8085|8088|8181|9000|9001|10000|10001'
```

Upstream ships the demo credentials `admin` / `password` for the MinIO root user and
the AWS access keys; the Superset login and `SUPERSET_SECRET_KEY` are demo values of
the same kind. Reach the consoles over an SSH tunnel rather than exposing the ports on
a shared host.

## Trino

`trino/catalog/iceberg.properties` defines one catalog, `iceberg`, on the REST catalog
and MinIO. The schemas are the same as in Spark's `demo` catalog: `bronze`, `silver`,
`quarantine`, `gold`, `metadata`.

`trino/rules.json` applies file-based access control:

- every catalog is read-only;
- `crm` in `bronze`, `silver` and `quarantine` has no privileges, so it is absent
  from `SHOW TABLES` and `information_schema` and any query on it is denied. These
  tables hold unmasked email, phone and address; `gold.customer_360` has the masked
  columns;
- every other table allows `SELECT`.

```bash
docker compose exec trino trino
trino> SELECT segment, count(*) FROM iceberg.gold.customer_360 GROUP BY 1;
trino> SELECT snapshot_id, committed_at FROM iceberg.gold."customer_rfm$snapshots";
trino> SELECT count(*) FROM iceberg.gold.customer_rfm FOR VERSION AS OF <snapshot_id>;
```

Superset connects with `trino://superset@trino:8080/iceberg` from inside
`iceberg_net`; from the host the address is `trino://<user>@localhost:8085/iceberg`.

## Superset

`superset/Dockerfile` adds the Trino driver (`trino==0.340.0`) to
`apache/superset:6.1.0`. Superset keeps its users and dashboards in SQLite in the
`superset_home` volume. Open http://localhost:8088 and log in as `admin` / `admin`.

`superset/dashboards/` holds the six dashboards with their charts, datasets and
database connection, in Superset's export format. `superset/bootstrap.sh` imports them
when it starts on an empty `superset_home` volume, and only then, so changes made in
the UI survive a restart.

| Task | Command, from `infra/` |
|---|---|
| write UI changes back to the repository | `docker compose exec superset bash /app/assets.sh export` |
| replace the dashboards with the repository version | `docker compose exec superset bash /app/assets.sh import` |
| start from an empty Superset | `docker compose rm -sf superset && docker volume rm infra_superset_home && docker compose up -d superset` |

An export from another Superset instance changes the numeric ids and colour lists in
the files; chart definitions, layout and datasets stay the same.

## Notes

`./warehouse` resolves to `infra/warehouse`, a runtime artefact covered by
`warehouse/` in `.gitignore`.

Spark's Iceberg metadata tables, such as `demo.gold.customer_rfm.snapshots`, are
rejected by the REST catalog with HTTP 400. Query snapshots through Trino as above.

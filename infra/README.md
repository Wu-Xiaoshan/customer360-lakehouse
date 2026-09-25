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
docker compose up -d
docker compose ps          # four services running
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

Four services on the `iceberg_net` network.

| Service | Port | Purpose |
|---|---|---|
| spark-iceberg | 8888 | Jupyter |
| spark-iceberg | 8080 | Spark UI |
| spark-iceberg | 10000, 10001 | Thrift server |
| rest | 8181 | Iceberg REST catalog |
| minio | 9000 | S3 API |
| minio | 9001 | MinIO console |

Check for conflicts before starting, particularly on 8080:

```bash
ss -tlnp | grep -E '8888|8080|8181|9000|9001|10000|10001'
```

Upstream ships the demo credentials `admin` / `password` for the MinIO root user and
the AWS access keys. Reach the consoles over an SSH tunnel rather than exposing the
ports on a shared host.

## Planned additions

| Step | Service | Purpose |
|---|---|---|
| 5 | Trino | SQL query layer over the Iceberg REST catalog |
| 6 | Superset | dashboards on the Gold tables, via Trino |

## Notes

`./warehouse` resolves to `infra/warehouse`, a runtime artefact covered by
`warehouse/` in `.gitignore`.

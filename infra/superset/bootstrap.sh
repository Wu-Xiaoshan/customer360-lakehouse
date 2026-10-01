#!/bin/bash
# Container entrypoint: initialise the metadata database, import the dashboards on
# the first start only, then run the web server.
set -e

superset db upgrade
superset fab create-admin --username admin --password admin \
  --firstname Admin --lastname User --email admin@example.com || true
superset init

marker="$SUPERSET_HOME/.dashboards_imported"
if [ ! -f "$marker" ] && [ -f /app/assets/dashboards/metadata.yaml ]; then
  bash /app/assets.sh import
  touch "$marker"
fi

exec /usr/bin/run-server.sh

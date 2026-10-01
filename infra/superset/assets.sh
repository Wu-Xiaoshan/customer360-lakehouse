#!/bin/bash
# Runs inside the superset container.
#   import  load infra/superset/dashboards into Superset, replacing what is there
#   export  write every dashboard with its charts, datasets and database connection
#           back to infra/superset/dashboards
set -e
dir=/app/assets/dashboards
work=$(mktemp -d)

case "$1" in
  import)
    cp -r "$dir" "$work/customer360"
    (cd "$work" && python -m zipfile -c bundle.zip customer360)
    superset import-dashboards -p "$work/bundle.zip" -u admin
    ;;
  export)
    superset export-dashboards -f "$work/bundle.zip"
    python -m zipfile -e "$work/bundle.zip" "$work/out"
    find "$dir" -mindepth 1 -delete
    cp -r "$work"/out/*/. "$dir"/
    # drop the numeric id that Superset appends to each file name; ids differ
    # between instances, names do not
    find "$dir" -name '*_[0-9]*.yaml' | while read -r f; do
      n=$(echo "$f" | sed -E 's/_[0-9]+\.yaml$/.yaml/')
      [ "$n" = "$f" ] && continue
      if [ -e "$n" ]; then echo "kept $f: $n exists" >&2; else mv "$f" "$n"; fi
    done
    ;;
  *)
    echo "usage: assets.sh import|export" >&2
    exit 1
    ;;
esac
rm -rf "$work"

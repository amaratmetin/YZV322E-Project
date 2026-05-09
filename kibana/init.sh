#!/bin/sh
set -eu

KIBANA_URL="${KIBANA_URL:-http://kibana:5601}"
SAVED_OBJECTS_FILE="${SAVED_OBJECTS_FILE:-/kibana/saved_objects.ndjson}"

echo "Waiting for Kibana at ${KIBANA_URL}..."
attempt=0
until curl -fs "${KIBANA_URL}/api/status" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "${attempt}" -gt 60 ]; then
    echo "Kibana did not become available after $((attempt * 5))s" >&2
    exit 1
  fi
  sleep 5
done

echo "Importing Kibana saved objects from ${SAVED_OBJECTS_FILE}..."
curl -fsS -X POST "${KIBANA_URL}/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  --form "file=@${SAVED_OBJECTS_FILE}"

echo
echo "Kibana init complete."

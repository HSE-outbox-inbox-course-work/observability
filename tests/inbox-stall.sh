#!/bin/bash
# Останавливает inbox на $DOWNTIME при фоновой нагрузке и поднимает обратно.
set -euo pipefail

DOWNTIME=${DOWNTIME:-45s}
RPS=${RPS:-5}

../loadgen/bin/loadgen --scenario=transfer --rps="$RPS" --duration="$DOWNTIME" \
  --target=http://localhost:8080 &
LOADGEN_PID=$!
trap 'kill $LOADGEN_PID 2>/dev/null || true' EXIT

echo "stopping inbox..."
docker compose -f ../docker-compose.yaml stop inbox >/dev/null

wait $LOADGEN_PID

echo "starting inbox..."
docker compose -f ../docker-compose.yaml start inbox >/dev/null

printf "waiting for inbox..."
until curl -sf http://localhost:8082/metrics >/dev/null 2>&1; do printf "."; sleep 1; done
echo " ok"

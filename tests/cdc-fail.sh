#!/bin/bash
# Останавливает Kafka Connect (Debezium перестаёт читать WAL), держит
# фоновый трафик на $DOWNTIME, потом включает Connect обратно и
# перерегистрирует коннектор.
set -euo pipefail

DOWNTIME=${DOWNTIME:-45s}
RPS=${RPS:-5}

../loadgen/bin/loadgen --scenario=transfer --rps="$RPS" --duration="$DOWNTIME" \
  --target=http://localhost:8080 &
LOADGEN_PID=$!
trap 'kill $LOADGEN_PID 2>/dev/null || true' EXIT

echo "stopping kafka-connect..."
docker compose -f ../docker-compose.yaml stop kafka-connect >/dev/null

wait $LOADGEN_PID

echo "starting kafka-connect..."
docker compose -f ../docker-compose.yaml start kafka-connect >/dev/null

make -C .. -s register-connector

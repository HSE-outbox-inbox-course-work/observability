#!/bin/bash
# cdc-fail останавливает Kafka Connect (Debezium перестаёт читать WAL),
# держит фоновый трафик 5 RPS на $DOWNTIME, потом возвращает Connect и
# перезапускает коннектор. Цель — увидеть, как outbox-таблица копит
# события и WAL slot растёт, а после восстановления всё уходит обратно.
set -euo pipefail

DOWNTIME=${DOWNTIME:-45s}
RPS=${RPS:-5}

# Фоновый трафик. Если Ctrl+C — kill его перед выходом.
..//loadgen/bin/loadgen --scenario=transfer --rps="$RPS" --duration="$DOWNTIME" \
  --target=http://localhost:8080 &
LOADGEN_PID=$!
trap 'kill $LOADGEN_PID 2>/dev/null || true' EXIT

echo "stopping kafka-connect..."
docker compose stop kafka-connect >/dev/null

echo "kafka-connect down. Traffic continues for $DOWNTIME"
echo "watch: Outbox/Outbox depth, Outbox/Oldest event age, Infra/Replication slot WAL lag"

wait $LOADGEN_PID

echo "starting kafka-connect..."
docker compose start kafka-connect >/dev/null

# Дать Connect подняться и снова зарегистрировать коннектор.
make -s register-connector

echo "done. Outbox depth should drain back to zero in a few seconds"

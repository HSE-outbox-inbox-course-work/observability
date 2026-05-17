#!/bin/bash
# Останавливает inbox, сбрасывает offsets группы inbox-service к началу
# топика, запускает inbox обратно. При повторном чтении почти все
# сообщения уже PROCESSED — растёт inbox_duplicates_total.
set -euo pipefail

echo "stopping inbox..."
docker compose -f ../docker-compose.yaml stop inbox >/dev/null

echo "resetting offsets of consumer group inbox-service to earliest..."
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group inbox-service --reset-offsets --to-earliest \
  --topic accounts.money.transferred --execute >/dev/null

echo "starting inbox..."
docker compose -f ../docker-compose.yaml start inbox >/dev/null

printf "waiting for inbox..."
until curl -sf http://localhost:8082/metrics >/dev/null 2>&1; do printf "."; sleep 1; done
echo " ok"

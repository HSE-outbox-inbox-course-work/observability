#!/bin/bash
# Останавливает inbox, сбрасывает offsets группы inbox-service к началу
# топика, запускает inbox обратно. При повторном чтении почти все
# сообщения уже PROCESSED — растёт inbox_duplicates_total.
set -euo pipefail

echo "stopping inbox..."
docker compose -f ../docker-compose.yaml stop inbox >/dev/null

# kafka-consumer-groups --reset-offsets срабатывает только когда группа
# Empty. После SIGTERM Kafka держит сессию до session.timeout.ms (у
# segmentio/kafka-go это 30+ секунд) и команда возвращает exit code 0
# с "Error: ..." на stderr — поэтому простой sleep + set -e не ловит
# проблему. Пробуем reset в цикле, пока не получится.

echo "resetting offsets to earliest (retrying until group becomes Empty)..."
deadline=$((SECONDS + 90))
while :; do
  out=$(docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
    --group inbox-service --reset-offsets --to-earliest \
    --topic accounts.money.transferred --execute 2>&1)
  if ! echo "$out" | grep -q "Error:"; then
    echo "$out"
    break
  fi
  if [ $SECONDS -ge $deadline ]; then
    echo "$out"
    echo "TIMEOUT waiting for consumer group to become Empty"
    exit 1
  fi
  printf "."
  sleep 3
done
echo

echo "starting inbox..."
docker compose -f ../docker-compose.yaml start inbox >/dev/null

printf "waiting for inbox..."
until curl -sf http://localhost:8082/metrics >/dev/null 2>&1; do printf "."; sleep 1; done
echo " ok"

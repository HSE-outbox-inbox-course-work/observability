#!/bin/bash
# dlq: публикует битые сообщения напрямую в accounts.money.transferred,
# минуя outbox-сервис. Inbox прогоняет их через validate(), помечает
# inbox_order как FAILED и шлёт в DLQ.
set -euo pipefail

COUNT=${COUNT:-20}

exec ../loadgen/bin/loadgen \
  --scenario=invalid-payload \
  --broker=localhost:29092 \
  --topic=accounts.money.transferred \
  --count="$COUNT"

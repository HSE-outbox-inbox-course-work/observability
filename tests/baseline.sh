#!/bin/bash
# baseline: спокойный поток 5 RPS на 30 секунд.
set -euo pipefail

DURATION=${DURATION:-30s}
RPS=${RPS:-5}

exec ../loadgen/bin/loadgen \
  --scenario=transfer \
  --rps="$RPS" \
  --duration="$DURATION" \
  --target=http://localhost:8080

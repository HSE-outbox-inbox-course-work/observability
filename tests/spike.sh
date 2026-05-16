#!/bin/bash
# spike: RPS 5 -> 50 -> 5 за 60 секунд.
set -euo pipefail

DURATION=${DURATION:-60s}
BASE=${BASE:-5}
PEAK=${PEAK:-50}

exec ../loadgen/bin/loadgen \
  --scenario=spike \
  --rps="$BASE" \
  --spike-rps="$PEAK" \
  --duration="$DURATION" \
  --target=http://localhost:8080

#!/usr/bin/env bash
set -euo pipefail

ARGS=("$@")
OUTPUT_PATH=${ARGS[0]:-stats.csv}
INTERVAL_SECONDS=${ARGS[1]:-1}
DURATION_SECONDS=""
START_INDEX=2

if [ ${#ARGS[@]} -ge 3 ]; then
  if [[ ${ARGS[2]} =~ ^[0-9]+$ ]]; then
    DURATION_SECONDS=${ARGS[2]}
    START_INDEX=3
  fi
fi

CONTAINER_ARGS=("${ARGS[@]:$START_INDEX}")

if [ ${#CONTAINER_ARGS[@]} -eq 0 ]; then
  CONTAINERS=(benchmark-platformatic benchmark-confluent)
else
  CONTAINERS=("${CONTAINER_ARGS[@]}")
fi

echo "timestamp,container,cpu_percent,mem_usage,mem_percent,net_io,block_io,pids" > "$OUTPUT_PATH"

collect_once() {
  local timestamp
  timestamp=$(date -Is)
  docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}" "${CONTAINERS[@]}" |
    while IFS= read -r line; do
      echo "${timestamp},${line}" >> "$OUTPUT_PATH"
    done
}

if [ -z "$DURATION_SECONDS" ] || [ "$DURATION_SECONDS" -le 0 ]; then
  while true; do
    collect_once
    sleep "$INTERVAL_SECONDS"
  done
else
  end=$((SECONDS + DURATION_SECONDS))
  while [ "$SECONDS" -lt "$end" ]; do
    collect_once
    sleep "$INTERVAL_SECONDS"
  done
fi

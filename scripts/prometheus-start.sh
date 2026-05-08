#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$ROOT_DIR/.local/data/prometheus"
LOG_FILE="$ROOT_DIR/.local/logs/prometheus.log"
PID_FILE="$ROOT_DIR/.local/pids/prometheus.pid"
CONFIG="$SCRIPT_DIR/prometheus.yml"
PORT=9090

mkdir -p "$DATA_DIR" "$ROOT_DIR/.local/logs" "$ROOT_DIR/.local/pids"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "✅ Prometheus already running (PID $(cat "$PID_FILE"))"
  exit 0
fi

echo "Starting Prometheus..."
prometheus \
  --config.file="$CONFIG" \
  --storage.tsdb.path="$DATA_DIR" \
  --web.listen-address=":$PORT" \
  --log.level=warn \
  >>"$LOG_FILE" 2>&1 &

echo $! >"$PID_FILE"

# Wait for HTTP
for _ in $(seq 1 20); do
  if curl -sf "http://localhost:$PORT/-/ready" >/dev/null 2>&1; then
    echo "✅ Prometheus ready!"
    echo "   UI:  http://localhost:$PORT"
    echo "   Log: $LOG_FILE"
    echo "   PID: $(cat "$PID_FILE")"
    exit 0
  fi
  sleep 0.5
done

echo "❌ Prometheus did not become ready — check $LOG_FILE"
exit 1

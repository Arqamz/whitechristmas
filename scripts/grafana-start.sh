#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$ROOT_DIR/.local/data/grafana"
LOG_FILE="$ROOT_DIR/.local/logs/grafana.log"
PID_FILE="$ROOT_DIR/.local/pids/grafana.pid"
PROV_DIR="$ROOT_DIR/.local/grafana-provisioning"
PORT=3001 # 3000 is taken by Next.js

mkdir -p "$DATA_DIR" "$ROOT_DIR/.local/logs" "$ROOT_DIR/.local/pids"
mkdir -p "$PROV_DIR/datasources" "$PROV_DIR/dashboards"

# Provision Prometheus datasource
cat >"$PROV_DIR/datasources/prometheus.yaml" <<EOF
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    url: http://localhost:9090
    isDefault: true
    editable: false
EOF

# Provision dashboard folder pointer
cat >"$PROV_DIR/dashboards/default.yaml" <<EOF
apiVersion: 1
providers:
  - name: WhiteChristmas
    folder: WhiteChristmas
    type: file
    options:
      path: $PROV_DIR/dashboards
EOF

# Write dashboard JSON
cat >"$PROV_DIR/dashboards/whitechristmas.json" <<'DASHBOARD'
{
  "title": "WhiteChristmas — Pipeline Metrics",
  "uid": "wc-pipeline",
  "schemaVersion": 36,
  "refresh": "5s",
  "panels": [
    {
      "id": 1, "gridPos": {"x":0,"y":0,"w":6,"h":4},
      "type": "stat", "title": "Events Total",
      "targets": [{"expr": "sum(whitechristmas_events_total)", "legendFormat": ""}],
      "options": {"colorMode": "background", "graphMode": "area"}
    },
    {
      "id": 2, "gridPos": {"x":6,"y":0,"w":6,"h":4},
      "type": "stat", "title": "Active WebSocket Clients",
      "targets": [{"expr": "whitechristmas_websocket_clients", "legendFormat": ""}],
      "options": {"colorMode": "background", "graphMode": "none"}
    },
    {
      "id": 3, "gridPos": {"x":12,"y":0,"w":6,"h":4},
      "type": "stat", "title": "Kafka Messages Consumed",
      "targets": [{"expr": "whitechristmas_kafka_messages_total", "legendFormat": ""}],
      "options": {"colorMode": "background", "graphMode": "area"}
    },
    {
      "id": 4, "gridPos": {"x":18,"y":0,"w":6,"h":4},
      "type": "stat", "title": "Broadcast Errors",
      "targets": [{"expr": "whitechristmas_broadcast_errors_total", "legendFormat": ""}],
      "options": {"colorMode": "background", "graphMode": "none"}
    },
    {
      "id": 5, "gridPos": {"x":0,"y":4,"w":12,"h":8},
      "type": "timeseries", "title": "Events / Second (by Severity)",
      "targets": [{"expr": "rate(whitechristmas_events_total[1m])", "legendFormat": "sev {{severity}}"}],
      "options": {"legend": {"displayMode": "list"}}
    },
    {
      "id": 6, "gridPos": {"x":12,"y":4,"w":12,"h":8},
      "type": "timeseries", "title": "HTTP Request Latency (p99)",
      "targets": [{"expr": "histogram_quantile(0.99, rate(whitechristmas_http_request_duration_seconds_bucket[1m]))", "legendFormat": "p99 {{route}}"}]
    }
  ],
  "time": {"from": "now-15m", "to": "now"}
}
DASHBOARD

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "✅ Grafana already running (PID $(cat "$PID_FILE"))"
  exit 0
fi

echo "Starting Grafana..."
GF_PATHS_DATA="$DATA_DIR" \
  GF_PATHS_LOGS="$ROOT_DIR/.local/logs" \
  GF_PATHS_PROVISIONING="$PROV_DIR" \
  GF_SERVER_HTTP_PORT="$PORT" \
  GF_AUTH_ANONYMOUS_ENABLED="true" \
  GF_AUTH_ANONYMOUS_ORG_ROLE="Admin" \
  GF_SECURITY_ADMIN_PASSWORD="admin" \
  GF_LOG_LEVEL="warn" \
  grafana-server \
  --homepath "$(dirname "$(which grafana-server)")/../share/grafana" \
  >>"$LOG_FILE" 2>&1 &

echo $! >"$PID_FILE"

for _ in $(seq 1 30); do
  if curl -sf "http://localhost:$PORT/api/health" >/dev/null 2>&1; then
    echo "✅ Grafana ready!"
    echo "   UI:   http://localhost:$PORT  (no login required)"
    echo "   Log:  $LOG_FILE"
    echo "   PID:  $(cat "$PID_FILE")"
    exit 0
  fi
  sleep 0.5
done

echo "❌ Grafana did not become ready — check $LOG_FILE"
exit 1

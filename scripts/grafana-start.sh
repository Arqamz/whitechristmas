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

# Write dashboard JSON (mirrors docker/grafana/dashboards/whitechristmas.json)
cat >"$PROV_DIR/dashboards/whitechristmas.json" <<'DASHBOARD'
{
  "title": "WhiteChristmas Pipeline",
  "uid": "whitechristmas",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "5s",
  "time": { "from": "now-30m", "to": "now" },
  "panels": [
    {
      "id": 1, "type": "stat", "title": "Total Events Ingested",
      "gridPos": { "x": 0, "y": 0, "w": 6, "h": 4 },
      "options": { "reduceOptions": { "calcs": ["lastNotNull"] }, "colorMode": "background" },
      "targets": [{ "expr": "sum(whitechristmas_events_total)", "legendFormat": "total" }]
    },
    {
      "id": 2, "type": "stat", "title": "Kafka Messages Consumed",
      "gridPos": { "x": 6, "y": 0, "w": 6, "h": 4 },
      "options": { "reduceOptions": { "calcs": ["lastNotNull"] }, "colorMode": "background" },
      "targets": [{ "expr": "sum(whitechristmas_kafka_messages_total)", "legendFormat": "messages" }]
    },
    {
      "id": 3, "type": "stat", "title": "WebSocket Clients",
      "gridPos": { "x": 12, "y": 0, "w": 6, "h": 4 },
      "options": { "reduceOptions": { "calcs": ["lastNotNull"] }, "colorMode": "value" },
      "targets": [{ "expr": "whitechristmas_websocket_clients", "legendFormat": "clients" }]
    },
    {
      "id": 4, "type": "stat", "title": "Broadcast Errors",
      "gridPos": { "x": 18, "y": 0, "w": 6, "h": 4 },
      "options": { "reduceOptions": { "calcs": ["lastNotNull"] }, "colorMode": "background" },
      "fieldConfig": { "defaults": { "thresholds": { "steps": [{ "color": "green", "value": 0 }, { "color": "red", "value": 1 }] } } },
      "targets": [{ "expr": "whitechristmas_broadcast_errors_total", "legendFormat": "errors" }]
    },
    {
      "id": 5, "type": "timeseries", "title": "Event Ingestion Rate \u2014 by Severity",
      "gridPos": { "x": 0, "y": 4, "w": 12, "h": 8 },
      "options": { "legend": { "displayMode": "list", "placement": "bottom" } },
      "targets": [{ "expr": "sum by(severity) (rate(whitechristmas_events_total[1m]))", "legendFormat": "severity {{severity}}" }]
    },
    {
      "id": 6, "type": "timeseries", "title": "Event Ingestion Rate \u2014 by Dataset",
      "gridPos": { "x": 12, "y": 4, "w": 12, "h": 8 },
      "options": { "legend": { "displayMode": "list", "placement": "bottom" } },
      "targets": [{ "expr": "sum by(dataset) (rate(whitechristmas_events_total[1m]))", "legendFormat": "{{dataset}}" }]
    },
    {
      "id": 7, "type": "timeseries", "title": "Kafka Alert Consumption Rate \u2014 by Topic",
      "gridPos": { "x": 0, "y": 12, "w": 12, "h": 8 },
      "options": { "legend": { "displayMode": "list", "placement": "bottom" } },
      "targets": [{ "expr": "sum by(topic) (rate(whitechristmas_kafka_messages_total[1m]))", "legendFormat": "{{topic}}" }]
    },
    {
      "id": 8, "type": "timeseries", "title": "HTTP Request Rate (by route)",
      "gridPos": { "x": 12, "y": 12, "w": 12, "h": 8 },
      "targets": [{ "expr": "sum by(route) (rate(whitechristmas_http_request_duration_seconds_count[1m]))", "legendFormat": "{{route}}" }]
    },
    {
      "id": 11, "type": "stat", "title": "Events \u2014 crimes",
      "gridPos": { "x": 0, "y": 20, "w": 6, "h": 4 },
      "options": { "reduceOptions": { "calcs": ["lastNotNull"] }, "colorMode": "background", "graphMode": "area" },
      "targets": [{ "expr": "sum(whitechristmas_events_total{dataset=\"crimes\"})", "legendFormat": "crimes" }]
    },
    {
      "id": 12, "type": "stat", "title": "Events \u2014 violence",
      "gridPos": { "x": 6, "y": 20, "w": 6, "h": 4 },
      "options": { "reduceOptions": { "calcs": ["lastNotNull"] }, "colorMode": "background", "graphMode": "area" },
      "targets": [{ "expr": "sum(whitechristmas_events_total{dataset=\"violence\"})", "legendFormat": "violence" }]
    },
    {
      "id": 13, "type": "stat", "title": "Events \u2014 arrests",
      "gridPos": { "x": 12, "y": 20, "w": 6, "h": 4 },
      "options": { "reduceOptions": { "calcs": ["lastNotNull"] }, "colorMode": "background", "graphMode": "area" },
      "targets": [{ "expr": "sum(whitechristmas_events_total{dataset=\"arrests\"})", "legendFormat": "arrests" }]
    },
    {
      "id": 14, "type": "stat", "title": "Events \u2014 sex-offenders",
      "gridPos": { "x": 18, "y": 20, "w": 6, "h": 4 },
      "options": { "reduceOptions": { "calcs": ["lastNotNull"] }, "colorMode": "background", "graphMode": "area" },
      "targets": [{ "expr": "sum(whitechristmas_events_total{dataset=\"sex-offenders\"})", "legendFormat": "sex-offenders" }]
    },
    {
      "id": 9, "type": "timeseries", "title": "HTTP P99 Latency (seconds)",
      "gridPos": { "x": 0, "y": 24, "w": 12, "h": 8 },
      "targets": [{ "expr": "histogram_quantile(0.99, sum by(route,le) (rate(whitechristmas_http_request_duration_seconds_bucket[1m])))", "legendFormat": "p99 {{route}}" }]
    },
    {
      "id": 10, "type": "timeseries", "title": "WebSocket Clients Over Time",
      "gridPos": { "x": 12, "y": 24, "w": 12, "h": 8 },
      "targets": [{ "expr": "whitechristmas_websocket_clients", "legendFormat": "connected clients" }]
    }
  ]
}
DASHBOARD

if [[ -f $PID_FILE ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
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
  grafana server \
  --homepath "$(dirname "$(which grafana)")/../share/grafana" \
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

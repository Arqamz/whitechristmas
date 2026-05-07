#!/usr/bin/env bash
# End-to-end integration test: CSV → Kafka → Spark → Go API → Frontend
# Usage: nix develop -c bash tests/e2e-test.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

PASS=0
FAIL=0

pass() {
  echo -e "  ${GREEN}✓${NC} $1"
  ((PASS++)) || true
}
fail() {
  echo -e "  ${RED}✗${NC} $1"
  ((FAIL++)) || true
}
section() { echo -e "\n${CYAN}━━━ $1 ━━━${NC}"; }

echo -e "${CYAN}╔══════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║  WhiteChristmas E2E Integration Test             ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════╝${NC}"

# ─── 1. Prerequisite checks ───────────────────────────────────────────────────
section "Prerequisites"

for cmd in nc curl go kafka-topics.sh kafka-console-producer.sh kafka-console-consumer.sh; do
  if command -v "$cmd" &>/dev/null; then
    pass "$cmd found"
  else
    fail "$cmd not found (are you in 'nix develop'?)"
  fi
done

DATA_FILE="$PROJECT_ROOT/data/Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings_20241231.csv"
if [ -f "$DATA_FILE" ]; then
  LINES=$(wc -l <"$DATA_FILE")
  pass "CSV data file found ($LINES lines)"
else
  fail "CSV data file not found at: $DATA_FILE"
fi

# ─── 2. Infrastructure checks ─────────────────────────────────────────────────
section "Infrastructure"

if nc -z localhost 2181 2>/dev/null; then
  pass "ZooKeeper reachable on :2181"
else
  fail "ZooKeeper not running — start with: bash scripts/kafka-start.sh"
fi

if nc -z localhost 9092 2>/dev/null; then
  pass "Kafka broker reachable on :9092"
else
  fail "Kafka broker not running — start with: bash scripts/kafka-start.sh"
fi

# Check Kafka topics exist
for topic in raw-events alerts; do
  if kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null | grep -q "^${topic}$"; then
    pass "Kafka topic '$topic' exists"
  else
    fail "Kafka topic '$topic' missing — will be auto-created on first produce"
  fi
done

# ─── 3. Go API health check ───────────────────────────────────────────────────
section "Go API Server"

if nc -z localhost 8081 2>/dev/null; then
  pass "Go API reachable on :8081"

  HEALTH=$(curl -s http://localhost:8081/health 2>/dev/null)
  if echo "$HEALTH" | grep -q '"status":"ok"'; then
    pass "Health endpoint returns ok"
  else
    fail "Health endpoint unexpected response: $HEALTH"
  fi

  STATS=$(curl -s http://localhost:8081/stats 2>/dev/null)
  if echo "$STATS" | grep -q '"connected_clients"'; then
    CLIENTS=$(echo "$STATS" | grep -o '"connected_clients":[0-9]*' | grep -o '[0-9]*$')
    pass "Stats endpoint ok ($CLIENTS WebSocket clients connected)"
  else
    fail "Stats endpoint unexpected response: $STATS"
  fi
else
  fail "Go API not running — start with: cd src/api && go run main.go"
fi

# ─── 4. Kafka round-trip test ─────────────────────────────────────────────────
section "Kafka Round-Trip"

TEST_EVENT=$(
  cat <<'EOF'
{"event_id":"test-e2e-00000001","victim_id":"V-TEST","incident_date":"2024-01-15","incident_time":"14:30:00","location":"123 TEST ST","district":"TEST","injury_type":"Non-Fatal Shooting","severity":4,"processed_timestamp":1705329000000}
EOF
)

echo "  Producing test event to raw-events..."
if echo "$TEST_EVENT" | kafka-console-producer.sh \
  --topic raw-events \
  --bootstrap-server localhost:9092 \
  2>/dev/null; then
  pass "Test event published to raw-events"
else
  fail "Failed to publish to raw-events"
fi

# Give Spark time to process if running
sleep 2

echo "  Checking raw-events topic has messages..."
MSG_COUNT=$(kafka-console-consumer.sh \
  --topic raw-events \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 100 \
  --timeout-ms 3000 \
  2>/dev/null | wc -l || true)

if [ "$MSG_COUNT" -gt 0 ]; then
  pass "raw-events topic has $MSG_COUNT messages"
else
  fail "raw-events topic appears empty"
fi

# ─── 5. Go producer build check ───────────────────────────────────────────────
section "Go Producer"

cd "$PROJECT_ROOT/src/kafka-producer"
if go build -o /tmp/wc-producer-test . 2>/dev/null; then
  pass "Go producer builds successfully"
  rm -f /tmp/wc-producer-test
else
  fail "Go producer build failed"
fi
cd "$PROJECT_ROOT"

# ─── 6. Go API server build check ─────────────────────────────────────────────
section "Go API"

cd "$PROJECT_ROOT/src/api"
if go build -o /tmp/wc-api-test . 2>/dev/null; then
  pass "Go API builds successfully"
  rm -f /tmp/wc-api-test
else
  fail "Go API build failed"
fi
cd "$PROJECT_ROOT"

# ─── 7. Frontend build check ──────────────────────────────────────────────────
section "Next.js Frontend"

cd "$PROJECT_ROOT/src/frontend"
if [ -d node_modules ]; then
  pass "node_modules present"
else
  fail "node_modules missing — run: cd src/frontend && npm install"
fi

if node -e "require('./next.config.js')" 2>/dev/null; then
  pass "next.config.js is valid"
else
  fail "next.config.js has errors"
fi
cd "$PROJECT_ROOT"

# ─── Summary ──────────────────────────────────────────────────────────────────
echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
TOTAL=$((PASS + FAIL))
if [ "$FAIL" -eq 0 ]; then
  echo -e "${GREEN}✅ All $TOTAL checks passed!${NC}"
else
  echo -e "${YELLOW}Results: ${GREEN}$PASS passed${NC}, ${RED}$FAIL failed${NC} (of $TOTAL total)"
fi

echo ""
echo -e "${YELLOW}Manual end-to-end run order:${NC}"
echo "  1. bash scripts/kafka-start.sh            # ZooKeeper + Kafka"
echo "  2. cd src/api && go run main.go           # Go WebSocket API"
echo "  3. cd src/spark-jobs && sbt run           # Spark stream processor"
echo "  4. cd src/frontend && npm run dev         # Next.js dashboard"
echo "  5. cd src/kafka-producer && go run . \\   # Push CSV events"
echo "       -csv ../../data/Violence_Reduction*.csv -rate 5"
echo ""

[ "$FAIL" -eq 0 ]

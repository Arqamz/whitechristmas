#!/usr/bin/env bash
set -euo pipefail

# Schema Registry startup script — uses confluent-platform from Nix environment
# Run this: nix develop -c bash scripts/schema-registry-start.sh
#
# Listens on port 8082 to avoid conflict with the Go API on port 8081.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/.local/logs"
DATA_DIR="${PROJECT_ROOT}/.local/data"
CONFIG_FILE="${DATA_DIR}/schema-registry.properties"
PID_FILE="${DATA_DIR}/schema-registry.pid"

mkdir -p "$LOG_DIR" "$DATA_DIR"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}╔════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║  Starting Confluent Schema Registry   ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════╝${NC}"

# Verify we are inside the Nix environment
if ! command -v schema-registry-start &>/dev/null; then
  echo -e "${RED}❌ schema-registry-start not found.${NC}"
  echo "   Are you inside 'nix develop'?"
  echo "   Run: nix develop -c bash scripts/schema-registry-start.sh"
  exit 1
fi

# Check Kafka is reachable before starting
if ! nc -z localhost 9092 2>/dev/null; then
  echo -e "${RED}❌ Kafka is not reachable on localhost:9092${NC}"
  echo "   Start Kafka first: nix develop -c bash scripts/kafka-start.sh"
  exit 1
fi

# Bail if already running
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo -e "${YELLOW}⚠️  Schema Registry is already running (PID $(cat "$PID_FILE"))${NC}"
  echo "   To restart: bash scripts/schema-registry-stop.sh && bash scripts/schema-registry-start.sh"
  exit 0
fi

# Write config — port 8082 avoids collision with the Go WebSocket API (8081)
cat >"$CONFIG_FILE" <<EOF
listeners=http://0.0.0.0:8082
host.name=localhost
kafkastore.bootstrap.servers=PLAINTEXT://localhost:9092
kafkastore.topic=_schemas
kafkastore.replication.factor=1
kafkastore.topic.replication.factor=1
subject.name.strategy=io.confluent.kafka.serializers.subject.TopicNameStrategy
schema.registry.group.id=schema-registry
schema.registry.inter.instance.protocol=http
debug=false
EOF

echo -e "${GREEN}➤ Config written to: ${CONFIG_FILE}${NC}"
echo -e "${GREEN}➤ Starting Schema Registry on http://localhost:8082 ...${NC}"

# Start in background; tee to log file
schema-registry-start "$CONFIG_FILE" \
  >>"$LOG_DIR/schema-registry.log" 2>&1 &

SR_PID=$!
echo "$SR_PID" >"$PID_FILE"

# Wait for the HTTP listener to come up (up to 20 s)
echo -n "   Waiting for Schema Registry"
for _ in $(seq 1 20); do
  if curl -sf http://localhost:8082/subjects >/dev/null 2>&1; then
    echo ""
    break
  fi
  echo -n "."
  sleep 1
done

if ! curl -sf http://localhost:8082/subjects >/dev/null 2>&1; then
  echo ""
  echo -e "${RED}❌ Schema Registry did not start within 20 seconds.${NC}"
  echo "   Check the log: tail -50 ${LOG_DIR}/schema-registry.log"
  kill "$SR_PID" 2>/dev/null || true
  rm -f "$PID_FILE"
  exit 1
fi

echo -e "${GREEN}✅ Schema Registry ready!${NC}"
echo ""
echo -e "${YELLOW}📡 API:${NC}    http://localhost:8082"
echo -e "${YELLOW}📝 Log:${NC}    ${LOG_DIR}/schema-registry.log"
echo -e "${YELLOW}🔢 PID:${NC}    ${SR_PID}"
echo ""
echo -e "${YELLOW}Quick checks:${NC}"
echo "   curl http://localhost:8082/subjects"
echo "   curl http://localhost:8082/config"
echo ""
echo -e "${YELLOW}To stop:${NC}"
echo "   nix develop -c bash scripts/schema-registry-stop.sh"

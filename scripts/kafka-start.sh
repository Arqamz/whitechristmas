#!/usr/bin/env bash
set -euo pipefail

# Kafka KRaft startup script for local development
# Kafka 4.x dropped ZooKeeper — this uses KRaft (Kafka Raft) mode.
# Run this: nix develop -c bash scripts/kafka-start.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/.local/logs"
DATA_DIR="${PROJECT_ROOT}/.local/data"
CLUSTER_ID_FILE="${DATA_DIR}/kafka-cluster-id"

mkdir -p "$LOG_DIR" "$DATA_DIR/kafka"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}╔════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║  Starting Kafka (KRaft mode)          ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════╝${NC}"

# Verify nix develop environment
if [ -z "${KAFKA_HOME:-}" ]; then
  echo -e "${RED}❌ KAFKA_HOME not set. Are you in 'nix develop'?${NC}"
  echo "  nix develop -c bash scripts/kafka-start.sh"
  exit 1
fi

# Check if already running
if nc -z localhost 9092 2>/dev/null; then
  echo -e "${YELLOW}⚠️  Kafka is already running on port 9092${NC}"
  echo "   To restart: bash scripts/kafka-stop.sh && bash scripts/kafka-start.sh"
  exit 0
fi

# Generate KRaft server.properties
echo -e "${GREEN}➤ Writing KRaft server.properties...${NC}"
cat >"$DATA_DIR/server.properties" <<EOF
# KRaft combined broker+controller (single-node dev setup)
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093

# Listeners
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://localhost:9092
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# Storage
log.dirs=$DATA_DIR/kafka

# Broker tuning
num.network.threads=4
num.io.threads=4
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=1
num.recovery.threads.per.data.dir=1

# Replication (single-node)
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
default.replication.factor=1

# Retention
log.retention.hours=24
log.segment.bytes=1073741824
log.cleanup.policy=delete
delete.topic.enable=true
auto.create.topics.enable=true
group.initial.rebalance.delay.ms=0
EOF

# Generate or reuse cluster ID, then format storage
if [ -f "$CLUSTER_ID_FILE" ]; then
  CLUSTER_ID="$(cat "$CLUSTER_ID_FILE")"
  echo -e "${GREEN}➤ Reusing cluster ID: ${CLUSTER_ID}${NC}"
else
  CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
  echo "$CLUSTER_ID" >"$CLUSTER_ID_FILE"
  echo -e "${GREEN}➤ Generated cluster ID: ${CLUSTER_ID}${NC}"
fi

# Format storage (safe to run repeatedly — skips if already formatted)
"$KAFKA_HOME/bin/kafka-storage.sh" format \
  --ignore-formatted \
  -t "$CLUSTER_ID" \
  -c "$DATA_DIR/server.properties" \
  >>"$LOG_DIR/kafka.log" 2>&1

# Start Kafka broker
echo -e "${GREEN}➤ Starting Kafka Broker (port 9092)...${NC}"
export LOG_DIR # kafka-server-start.sh uses LOG_DIR for its own logs
"$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$DATA_DIR/server.properties" \
  >>"$LOG_DIR/kafka.log" 2>&1

# Wait up to 15 s for the broker to become ready
echo -n "   Waiting for Kafka"
for i in $(seq 1 15); do
  if nc -z localhost 9092 2>/dev/null; then
    echo ""
    break
  fi
  echo -n "."
  sleep 1
done

if ! nc -z localhost 9092 2>/dev/null; then
  echo ""
  echo -e "${RED}❌ Kafka failed to start within 15 seconds${NC}"
  echo ""
  echo -e "${YELLOW}Last 40 lines of Kafka log:${NC}"
  tail -40 "$LOG_DIR/kafka.log" || echo "  (No log available)"
  exit 1
fi

echo -e "${GREEN}✓ Kafka Broker started successfully${NC}"

# Create topics
echo -e "${GREEN}➤ Creating Kafka topics...${NC}"
for topic in raw-events processed-events alerts dead-letter; do
  if "$KAFKA_HOME/bin/kafka-topics.sh" \
    --create \
    --topic "$topic" \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists \
    2>/dev/null; then
    echo "  ✓ Topic '$topic' created"
  else
    echo "  ℹ️  Topic '$topic' already exists"
  fi
done

echo -e "${CYAN}════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Kafka ready!${NC}"
echo ""
echo -e "${YELLOW}📋 Topics:${NC}"
"$KAFKA_HOME/bin/kafka-topics.sh" --list --bootstrap-server localhost:9092 | sed 's/^/   /'
echo ""
echo -e "${YELLOW}📊 Broker Info:${NC}"
echo "   Broker:     PLAINTEXT://localhost:9092"
echo "   Controller: localhost:9093 (KRaft)"
echo ""
echo -e "${YELLOW}📝 Log:${NC}"
echo "   $LOG_DIR/kafka.log"
echo ""
echo -e "${YELLOW}📡 Quick Commands:${NC}"
echo "   kafka-topics.sh --list --bootstrap-server localhost:9092"
echo "   kafka-topics.sh --describe --topic raw-events --bootstrap-server localhost:9092"
echo "   echo 'test' | kafka-console-producer.sh --topic raw-events --bootstrap-server localhost:9092"
echo "   kafka-console-consumer.sh --topic raw-events --bootstrap-server localhost:9092 --from-beginning"
echo ""
echo -e "${YELLOW}🛑 To stop:${NC}"
echo "   nix develop -c bash scripts/kafka-stop.sh"

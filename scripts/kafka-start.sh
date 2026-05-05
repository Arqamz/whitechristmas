#!/usr/bin/env bash
set -euo pipefail

# Kafka & ZooKeeper startup script for local development

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/.local/logs"
DATA_DIR="${PROJECT_ROOT}/.local/data"

# Create directories
mkdir -p "$LOG_DIR" "$DATA_DIR/zookeeper" "$DATA_DIR/kafka"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}🚀 Starting Kafka Infrastructure${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Start ZooKeeper
echo -e "${GREEN}➤ Starting ZooKeeper...${NC}"
export SERVER_JVMFLAGS="-Dzookeeper.dataDir=$DATA_DIR/zookeeper"
zkServer.sh start 2>&1 | tee "$LOG_DIR/zookeeper.log" &
ZOOKEEPER_PID=$!
sleep 3

# Start Kafka Broker
echo -e "${GREEN}➤ Starting Kafka Broker...${NC}"
KAFKA_LOG_DIR="$LOG_DIR" KAFKA_BROKER_RACK=1 kafka-server-start.sh \
  -daemon \
  <(cat <<EOF
broker.id=1
listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
log.dirs=$DATA_DIR/kafka
zookeeper.connect=localhost:2181
auto.create.topics.enable=true
num.partitions=1
default.replication.factor=1
delete.topic.enable=true
log.retention.hours=24
EOF
) 2>&1 | tee "$LOG_DIR/kafka.log"

sleep 3

# Create topics
echo -e "${GREEN}➤ Creating Kafka topics...${NC}"
kafka-topics.sh --create --topic raw-events --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1 2>/dev/null || echo "  (Topic may already exist)"
kafka-topics.sh --create --topic processed-events --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1 2>/dev/null || echo "  (Topic may already exist)"
kafka-topics.sh --create --topic alerts --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1 2>/dev/null || echo "  (Topic may already exist)"
kafka-topics.sh --create --topic dead-letter --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1 2>/dev/null || echo "  (Topic may already exist)"

echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✅ Kafka infrastructure ready!${NC}"
echo ""
echo -e "${YELLOW}Topics created:${NC}"
kafka-topics.sh --list --bootstrap-server localhost:9092

echo ""
echo -e "${YELLOW}📝 Logs:${NC}"
echo "  ZooKeeper: $LOG_DIR/zookeeper.log"
echo "  Kafka:     $LOG_DIR/kafka.log"
echo ""
echo -e "${YELLOW}To view logs in real-time:${NC}"
echo "  tail -f $LOG_DIR/zookeeper.log"
echo "  tail -f $LOG_DIR/kafka.log"

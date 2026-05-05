#!/usr/bin/env bash
set -euo pipefail

# Kafka & ZooKeeper startup script for local development
# Run this: nix develop -c bash scripts/kafka-start.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/.local/logs"
DATA_DIR="${PROJECT_ROOT}/.local/data"

# Create directories
mkdir -p "$LOG_DIR" "$DATA_DIR/zookeeper" "$DATA_DIR/kafka"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}╔════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║  🚀 Starting Kafka Infrastructure     ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════╝${NC}"

# Check if we're in nix develop environment
if [ -z "${KAFKA_HOME:-}" ]; then
    echo -e "${RED}❌ KAFKA_HOME not set. Are you in 'nix develop'?${NC}"
    echo ""
    echo -e "${CYAN}Run this command instead:${NC}"
    echo "  nix develop -c bash scripts/kafka-start.sh"
    exit 1
fi

# Start ZooKeeper
echo -e "${GREEN}➤ Starting ZooKeeper (port 2181)...${NC}"
mkdir -p "$DATA_DIR/zookeeper"

# Generate ZooKeeper config
cat > "$DATA_DIR/zookeeper.cfg" <<EOF
dataDir=$DATA_DIR/zookeeper
clientPort=2181
tickTime=2000
initLimit=5
syncLimit=2
4lw.commands.whitelist=*
EOF

# Start ZooKeeper with proper environment
export ZOO_LOG_DIR="$LOG_DIR"
export ZOO_LOG4J_PROP=INFO
zkServer.sh start "$DATA_DIR/zookeeper.cfg" > "$LOG_DIR/zookeeper.log" 2>&1

sleep 3

# Check if ZooKeeper started
if ! nc -z localhost 2181 2>/dev/null; then
    echo -e "${RED}❌ ZooKeeper failed to start${NC}"
    echo ""
    echo -e "${YELLOW}Last 30 lines of ZooKeeper log:${NC}"
    tail -30 "$LOG_DIR/zookeeper.log" || echo "  (No log available)"
    exit 1
fi

echo -e "${GREEN}✓ ZooKeeper started successfully${NC}"

# Start Kafka Broker
echo -e "${GREEN}➤ Starting Kafka Broker (port 9092)...${NC}"
mkdir -p "$DATA_DIR/kafka"

# Create server.properties dynamically
cat > "$DATA_DIR/server.properties" <<'KAFKA_CONFIG'
broker.id=1
listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=6000
log.dirs=KAFKA_DATA_DIR/kafka
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
delete.topic.enable=true
log.retention.hours=24
log.segment.bytes=1073741824
log.cleanup.policy=delete
auto.create.topics.enable=true
default.replication.factor=1
group.initial.rebalance.delay.ms=0
KAFKA_CONFIG

# Replace placeholder with actual path
sed -i "s|KAFKA_DATA_DIR|$DATA_DIR|g" "$DATA_DIR/server.properties"

# Start Kafka as a background process
"$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$DATA_DIR/server.properties" >> "$LOG_DIR/kafka.log" 2>&1

sleep 4

# Verify Kafka is running
if ! nc -z localhost 9092 2>/dev/null; then
    echo -e "${RED}❌ Kafka failed to start${NC}"
    echo ""
    echo -e "${YELLOW}Last 30 lines of Kafka log:${NC}"
    tail -30 "$LOG_DIR/kafka.log" || echo "  (No log available)"
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
echo -e "${GREEN}✅ Kafka infrastructure ready!${NC}"
echo ""
echo -e "${YELLOW}📋 Topics:${NC}"
"$KAFKA_HOME/bin/kafka-topics.sh" --list --bootstrap-server localhost:9092 | sed 's/^/   /'

echo ""
echo -e "${YELLOW}📊 Broker Info:${NC}"
echo "   Listeners: PLAINTEXT://localhost:9092"
echo "   ZooKeeper: localhost:2181"
echo ""
echo -e "${YELLOW}📝 Logs:${NC}"
echo "   Kafka:      $LOG_DIR/kafka.log"
echo "   ZooKeeper:  $LOG_DIR/zookeeper.log"
echo ""
echo -e "${YELLOW}📡 Quick Commands:${NC}"
echo "   # List topics"
echo "   kafka-topics.sh --list --bootstrap-server localhost:9092"
echo ""
echo "   # Describe a topic"
echo "   kafka-topics.sh --describe --topic raw-events --bootstrap-server localhost:9092"
echo ""
echo "   # Produce test message"
echo "   echo 'test' | kafka-console-producer.sh --topic raw-events --bootstrap-server localhost:9092"
echo ""
echo "   # Consume messages"
echo "   kafka-console-consumer.sh --topic raw-events --bootstrap-server localhost:9092 --from-beginning"
echo ""
echo -e "${YELLOW}🛑 To stop:${NC}"
echo "   nix develop -c bash scripts/kafka-stop.sh"

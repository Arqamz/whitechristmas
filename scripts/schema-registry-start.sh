#!/usr/bin/env bash
set -euo pipefail

# Schema Registry startup script for local development

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/.local/logs"
DATA_DIR="${PROJECT_ROOT}/.local/data"

mkdir -p "$LOG_DIR" "$DATA_DIR/schema-registry"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Check if Confluent CLI or schema-registry is available
if ! command -v schema-registry-start.sh &> /dev/null; then
    echo -e "${RED}❌ schema-registry-start.sh not found${NC}"
    echo ""
    echo -e "${YELLOW}📦 Schema Registry needs to be installed.${NC}"
    echo ""
    echo "For now, we'll generate a configuration file for manual setup or containerization."
    echo ""
    
    # Create config file
    cat > "$PROJECT_ROOT/.local/schema-registry.properties" <<EOF
# Confluent Schema Registry Configuration
listeners=http://0.0.0.0:8081
kafkastore.bootstrap.servers=localhost:9092
kafkastore.topic=_schemas
kafkastore.replication.factor=1
subject.name.strategy=io.confluent.kafka.serializers.subject.TopicNameStrategy
schema.registry.group.id=schema-registry
schema.registry.inter.instance.protocol=http
EOF
    
    echo -e "${GREEN}✅ Schema Registry config created at:${NC}"
    echo "   $PROJECT_ROOT/.local/schema-registry.properties"
    echo ""
    echo -e "${YELLOW}⚠️  Next steps:${NC}"
    echo "   1. Install Confluent Platform or build schema-registry from source"
    echo "   2. Or use Docker: docker run -d -p 8081:8081 confluentinc/cp-schema-registry:7.5.0"
    echo "   3. Then run this script again"
    
    exit 0
fi

echo -e "${YELLOW}🚀 Starting Schema Registry${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Create properties file
cat > "$DATA_DIR/schema-registry.properties" <<EOF
listeners=http://0.0.0.0:8081
kafkastore.bootstrap.servers=localhost:9092
kafkastore.topic=_schemas
kafkastore.replication.factor=1
subject.name.strategy=io.confluent.kafka.serializers.subject.TopicNameStrategy
schema.registry.group.id=schema-registry
schema.registry.inter.instance.protocol=http
log.dir=$DATA_DIR/schema-registry
EOF

echo -e "${GREEN}➤ Starting Schema Registry on localhost:8081...${NC}"
schema-registry-start.sh "$DATA_DIR/schema-registry.properties" \
  2>&1 | tee "$LOG_DIR/schema-registry.log" &

sleep 3

echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✅ Schema Registry ready!${NC}"
echo ""
echo -e "${YELLOW}📝 Log:${NC}"
echo "  $LOG_DIR/schema-registry.log"
echo ""
echo -e "${YELLOW}📡 API:${NC}"
echo "  http://localhost:8081"
echo ""
echo -e "${YELLOW}To view logs in real-time:${NC}"
echo "  tail -f $LOG_DIR/schema-registry.log"

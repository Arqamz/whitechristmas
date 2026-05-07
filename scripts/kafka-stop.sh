#!/usr/bin/env bash
set -euo pipefail

# Stop Kafka (KRaft mode — no ZooKeeper)
# Run this: nix develop -c bash scripts/kafka-stop.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${RED}Stopping Kafka${NC}"

pkill -f "kafka.Kafka" 2>/dev/null || true
sleep 1

echo -e "${GREEN}✅ Kafka infrastructure stopped${NC}"
echo ""
echo -e "${YELLOW}📁 Logs and data preserved at:${NC}"
echo "   ${PROJECT_ROOT}/.local/"
echo ""
echo -e "${YELLOW}💾 To clean up all logs and data:${NC}"
echo "   rm -rf ${PROJECT_ROOT}/.local/"

#!/usr/bin/env bash
set -euo pipefail

# Schema Registry stop script
# Run this: nix develop -c bash scripts/schema-registry-stop.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="${PROJECT_ROOT}/.local/data"
PID_FILE="${DATA_DIR}/schema-registry.pid"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${RED}🛑 Stopping Schema Registry${NC}"

if [ -f "$PID_FILE" ]; then
  SR_PID="$(cat "$PID_FILE")"
  if kill -0 "$SR_PID" 2>/dev/null; then
    kill "$SR_PID"
    echo -e "${GREEN}✅ Sent SIGTERM to PID ${SR_PID}${NC}"
  else
    echo -e "${YELLOW}⚠️  PID ${SR_PID} not running (already stopped?)${NC}"
  fi
  rm -f "$PID_FILE"
else
  # Fallback: kill by process name
  if pkill -f "schema-registry" 2>/dev/null; then
    echo -e "${GREEN}✅ Killed schema-registry processes${NC}"
  else
    echo -e "${YELLOW}⚠️  No schema-registry process found${NC}"
  fi
fi

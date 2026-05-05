#!/usr/bin/env bash
set -euo pipefail

# Stop Kafka & ZooKeeper

echo "🛑 Stopping Kafka infrastructure..."

# Kill Kafka
pkill -f kafka.Kafka || true
sleep 1

# Kill ZooKeeper
zkServer.sh stop || true
sleep 1

echo "✅ Kafka infrastructure stopped"

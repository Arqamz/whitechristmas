# WhiteChristmas — End-to-End Pipeline Run Guide

Trace a single crime event from raw CSV through every layer of the architecture and watch it appear live on the dashboard.

```
Rust Producer → Avro → Schema Registry → Kafka (raw-events)
  → Spark Streaming → Kafka (alerts)
  → Go WebSocket API → Next.js Dashboard
```

---

## Port Map

| Service          | Port | Notes                                         |
| ---------------- | ---- | --------------------------------------------- |
| Kafka Broker     | 9092 | PLAINTEXT                                     |
| Kafka Controller | 9093 | KRaft internal (no ZooKeeper)                 |
| Schema Registry  | 8082 | confluent-platform (avoids conflict with API) |
| Go API           | 8081 | WebSocket + HTTP + `/metrics`                 |
| Next.js          | 3000 | Dev server                                    |
| Prometheus       | 9090 | Scrapes Go API `/metrics` every 5 s           |
| Grafana          | 3001 | Pre-provisioned pipeline dashboard            |

> **Port conflict note:** The Schema Registry default is 8081, same as the Go API.
> `scripts/schema-registry-start.sh` configures it to listen on **8082**.
> All commands below reflect this.

---

## Prerequisites

Every terminal must be inside the Nix dev environment. `confluent-platform` (which
provides `schema-registry-start`) is installed there — no Docker required.

```bash
# Option A — direnv (auto-activates on cd)
direnv allow

# Option B — explicit shell
nix develop
```

Verify the environment is loaded:

```bash
kafka-topics.sh --version        # Kafka version
spark-submit --version           # Spark 4.0.x
schema-registry-start --help     # Confluent Schema Registry
go version                       # go1.26.x
cargo --version                  # cargo 1.x
node --version                   # v24.x
```

---

## Step 1 — Start Kafka (KRaft mode)

Open **Terminal 1**.

```bash
cd /home/arqam/WhiteChristmas
nix develop -c bash scripts/kafka-start.sh
```

Expected output ends with:

```
✅ Kafka ready!
   Topics: alerts  dead-letter  processed-events  raw-events
```

**Verify:**

```bash
nc -z localhost 9092 && echo "Kafka OK"
```

---

## Step 2 — Start Schema Registry

Open **Terminal 2**. Uses the `schema-registry-start` binary from `confluent-platform`.

```bash
cd /home/arqam/WhiteChristmas
nix develop -c bash scripts/schema-registry-start.sh
```

The script:

1. Writes `.local/data/schema-registry.properties` (listener on port **8082**)
2. Starts `schema-registry-start` in the background
3. Polls until the HTTP API responds, then exits

Expected output:

```
✅ Schema Registry ready!
   API:  http://localhost:8082
   Log:  .local/logs/schema-registry.log
   PID:  <pid>
```

**Verify:**

```bash
curl -s http://localhost:8082/subjects
# [] — empty array, no schemas registered yet

curl -s http://localhost:8082/config
# {"compatibilityLevel":"BACKWARD"}
```

**Tail logs if needed:**

```bash
tail -f .local/logs/schema-registry.log
```

---

## Step 3 — Start the Go WebSocket API

Open **Terminal 3**.

```bash
cd /home/arqam/WhiteChristmas/src/api

# Build (skip if binary is current)
go build -o api .

# Run
API_PORT=8081 KAFKA_BROKERS=localhost:9092 ALERTS_TOPIC=alerts ./api
```

Expected startup output:

```
Starting WhiteChristmas API server on :8081
Connecting to Kafka at localhost:9092, topic: alerts
```

**Verify:**

```bash
curl -s http://localhost:8081/health
# {"status":"ok","name":"whitechristmas-api","version":"0.1.0"}

curl -s http://localhost:8081/stats
# {"connected_clients":0,"total_events":0,"timestamp":...}
```

---

## Step 4 — Start the Next.js Dashboard

Open **Terminal 4**.

```bash
cd /home/arqam/WhiteChristmas/src/frontend
npm install        # first time only
npm run dev
```

Open **http://localhost:3000** in a browser.

You should see:

- Connected Clients: 0
- Total Events: 0
- Empty event feed

The dashboard auto-connects to `ws://localhost:8081/ws` and polls `/stats` every 2 seconds.

---

## Step 5 — Start Spark Streaming

Open **Terminal 5**. Spark consumes `raw-events` and produces to `alerts`.

```bash
cd /home/arqam/WhiteChristmas/src/spark-jobs

# Build the JAR (first time or after code changes)
sbt package

# Run with sbt (resolves all managed deps from local Ivy cache — no fat JAR needed)
KAFKA_BROKERS=localhost:9092 sbt run
```

Expected startup output:

```
╔════════════════════════════════════════════════════╗
║  🎯 WhiteChristmas Spark Stream Processor         ║
╚════════════════════════════════════════════════════╝
📊 Kafka Brokers: localhost:9092
📖 Input Topic:   raw-events
📤 Output Topic:  alerts
✓ Avro schema loaded
📥 Connecting to Kafka...
```

Spark blocks here, streaming continuously. Logs appear as events arrive.

---

## Step 6 — Run the Rust Producer (Single Event)

Open **Terminal 6**. This injects the single point transform.

```bash
cd /home/arqam/WhiteChristmas/src/kafka-producer

# Build (first time or after code changes)
cargo build --release

# Produce exactly 1 event, verbose
./target/release/producer \
  --csv-path ../../data/Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings_20260505.csv \
  --kafka-brokers localhost:9092 \
  --topic raw-events \
  --schema-registry http://localhost:8082 \
  --events-per-sec 1 \
  --max-events 1 \
  --verbose
```

Expected output:

```
[INFO] Fetching/registering schema with Schema Registry at http://localhost:8082
[INFO] Schema registered with ID: 1
[INFO] Reading CSV: ...Violence_Reduction...csv
[INFO] Publishing event_id=<uuid> date=... district=... severity=...
[INFO] ✓ Delivered to raw-events [partition=0, offset=0]
[INFO] Published 1/1 events. Exiting.
```

---

## Observing the Single Event Through Each Stage

### Stage 1 — Schema Registry (schema registered)

After the producer runs, the Avro schema is stored:

```bash
curl -s http://localhost:8082/subjects
# ["raw-events-value"]

curl -s http://localhost:8082/subjects/raw-events-value/versions/latest | python3 -m json.tool
# Full crime-event.avsc stored under schema ID 1
```

You can also use the Confluent Avro console consumer to decode the message directly:

```bash
kafka-avro-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic raw-events \
  --from-beginning \
  --max-messages 1 \
  --property schema.registry.url=http://localhost:8082
```

### Stage 2 — Kafka raw-events topic (raw Avro bytes)

```bash
kafka-console-consumer.sh \
  --topic raw-events \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 1 | xxd | head -5
```

Output starts with `00 00 00 00 01` — the 5-byte Confluent wire format header:
magic byte `0x00` + schema ID `0x00000001` (big-endian), followed by the Avro binary payload.

### Stage 3 — Spark (Terminal 5)

Within a few seconds Terminal 5 prints:

```
-------------------------------------------
Batch: 1
-------------------------------------------
+------------------+------------+-----------+----------+---------+
|event_id          |incident_date|district  |injury_type|severity|
+------------------+------------+-----------+----------+---------+
|<uuid>            |YYYY-MM-DD  |...        |...        |<int>   |
+------------------+------------+-----------+----------+---------+
```

If `severity >= 3`, Spark writes the event as JSON to the `alerts` topic.
If `severity < 3`, it is logged to console only.

> The Violence_Reduction dataset records are typically severity 3–5, so a single
> event will almost always reach `alerts`. If it doesn't, produce 5–10 events instead.

### Stage 4 — Kafka alerts topic (JSON)

```bash
kafka-console-consumer.sh \
  --topic alerts \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 1
```

Expected (one line of JSON):

```json
{
  "event_id": "<uuid>",
  "victim_id": null,
  "incident_date": "YYYY-MM-DD",
  "incident_time": "HH:MM",
  "location": "...",
  "district": "...",
  "injury_type": "...",
  "severity": 4,
  "processed_timestamp": 1234567890000
}
```

### Stage 5 — Go API (Terminal 3)

The API's Kafka consumer picks up the alert immediately. Terminal 3 logs:

```
Received event from Kafka: event_id=<uuid> severity=4
Broadcasting to 1 client(s)
```

Check updated stats:

```bash
curl -s http://localhost:8081/stats
# {"connected_clients":1,"total_events":1,"timestamp":...}
```

### Stage 6 — Next.js Dashboard (browser)

Watch **http://localhost:3000** update live:

- Total Events counter increments to 1
- Event card appears with: Event ID, date, district, injury type
- Severity badge color-coded: 1–2 green, 3 yellow, 4–5 red

---

## Producing a Continuous Stream

```bash
# 10 events/sec, run until interrupted
./target/release/producer \
  --csv-path ../../data/Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings_20260505.csv \
  --kafka-brokers localhost:9092 \
  --schema-registry http://localhost:8082 \
  --events-per-sec 10
```

---

## Observability (Prometheus + Grafana)

Start after the Go API is running:

```bash
# Terminal 7 — Prometheus (scrapes :8081/metrics every 5 s)
nix develop -c bash scripts/prometheus-start.sh
# UI: http://localhost:9090

# Terminal 8 — Grafana (pre-loaded WhiteChristmas dashboard)
nix develop -c bash scripts/grafana-start.sh
# UI: http://localhost:3001  (anonymous admin, no login)
```

**Pre-built dashboard** includes:

- Events total counter (by severity label)
- Active WebSocket clients gauge
- Kafka messages consumed rate
- HTTP request latency (p99 per route)
- Broadcast errors counter

---

## Time Slider (Historical Replay)

The dashboard footer has a **Time Replay** scrubber. Drag it left to load historical
events from PostgreSQL instead of the live WebSocket stream. A "PAUSED / Historical Replay"
badge appears in the feed header. Click **↩ Live** or drag the slider all the way right
to resume the real-time feed.

---

## Teardown

```bash
# Stop producer — Ctrl+C in Terminal 6
# Stop Spark   — Ctrl+C in Terminal 5
# Stop Go API  — Ctrl+C in Terminal 3
# Stop Next.js — Ctrl+C in Terminal 4

# Stop Grafana
nix develop -c bash scripts/grafana-stop.sh

# Stop Prometheus
nix develop -c bash scripts/prometheus-stop.sh

# Stop Schema Registry
nix develop -c bash scripts/schema-registry-stop.sh

# Stop Kafka
nix develop -c bash scripts/kafka-stop.sh
```

---

## Quick Diagnostics

| Symptom                              | Check                                                                                             |
| ------------------------------------ | ------------------------------------------------------------------------------------------------- |
| Producer can't reach Schema Registry | `curl http://localhost:8082/subjects`                                                             |
| Producer can't reach Kafka           | `nc -z localhost 9092`                                                                            |
| Spark sees no messages               | `kafka-console-consumer.sh --topic raw-events --bootstrap-server localhost:9092 --from-beginning` |
| Alerts topic empty                   | Check Spark terminal — severity may be < 3                                                        |
| Dashboard not updating               | Check `ws://localhost:8081/ws` in browser DevTools → Network tab                                  |
| Go API not consuming                 | `curl http://localhost:8081/stats` — total_events stuck at 0                                      |

**Log files:**

```
.local/logs/kafka.log
.local/logs/schema-registry.log
```

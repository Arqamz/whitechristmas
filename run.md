# WhiteChristmas — End-to-End Pipeline Run Guide

Trace crime events from raw CSV through every layer of the architecture and watch them appear live on the dashboard.

```
Rust Producer → Avro → Schema Registry → Kafka (raw-events-{dataset} × 4)
  → Spark StreamProcessor → Kafka (alerts-{dataset} × 4, anomaly-alerts)
  → Go WebSocket API → Next.js Dashboard
         ↕
  Spark BoltPipeline (parallel Storm-topology path, same raw-events-.*input)
         ↕
  PostgreSQL (events, alerts, batch analytics)  MongoDB (alerts, alert_logs)
```

---

## Port Map

| Service         | Port  | Notes                                         |
| --------------- | ----- | --------------------------------------------- |
| Kafka Broker    | 9092  | PLAINTEXT (internal + host)                   |
| Kafka Broker    | 29092 | PLAINTEXT_HOST (Docker external access)       |
| Kafka KRaft     | 9093  | Controller — no ZooKeeper                     |
| Schema Registry | 8082  | Confluent (avoids conflict with Go API :8081) |
| Go API          | 8081  | WebSocket + REST + `/metrics`                 |
| Next.js         | 3000  | Dashboard                                     |
| Prometheus      | 9090  | Scrapes Go API `/metrics` every 5 s           |
| Grafana         | 3001  | Pre-provisioned pipeline dashboard            |
| PostgreSQL      | 5432  | Batch analytics + events history              |
| MongoDB         | 27017 | Live alerts store                             |

---

## Topic Layout (10 topics total)

| Topic                      | Partitions | Format | Producer        | Consumer(s)                   |
| -------------------------- | ---------- | ------ | --------------- | ----------------------------- |
| `raw-events-crimes`        | 4          | Avro   | Rust producer   | StreamProcessor, BoltPipeline |
| `raw-events-violence`      | 4          | Avro   | Rust producer   | StreamProcessor, BoltPipeline |
| `raw-events-arrests`       | 4          | Avro   | Rust producer   | StreamProcessor, BoltPipeline |
| `raw-events-sex-offenders` | 4          | Avro   | Rust producer   | StreamProcessor, BoltPipeline |
| `alerts-crimes`            | 2          | JSON   | StreamProcessor | Go API                        |
| `alerts-violence`          | 2          | JSON   | StreamProcessor | Go API                        |
| `alerts-arrests`           | 2          | JSON   | StreamProcessor | Go API                        |
| `alerts-sex-offenders`     | 2          | JSON   | StreamProcessor | Go API                        |
| `anomaly-alerts`           | 2          | JSON   | StreamProcessor | (monitoring)                  |
| `dead-letter`              | 2          | —      | (future use)    | —                             |

> Spark subscribes to **all four** raw-events topics via `subscribePattern("raw-events-.*")`.
> Alert routing is per-row: `source_dataset` = `crimes|violence|arrests|sex-offenders`
> maps to `alerts-crimes|alerts-violence|alerts-arrests|alerts-sex-offenders`.

---

## Option A — Docker Compose (recommended, one command)

Starts every service with all dependencies in the correct order.

```bash
cd /home/arqam/WhiteChristmas

# Start the full stack
docker compose up --build

# Or detached (logs via docker compose logs -f)
docker compose up --build -d
```

Services started automatically:

1. `kafka` + `schema-registry`
2. `kafka-init` — creates all 10 topics then exits
3. `postgres` + `mongodb`
4. `spark-streaming` — StreamProcessor + BoltPipeline
5. `api` — Go WebSocket API
6. `producer` — Rust producer, parallel infinite mode, 10 events/sec per dataset (~40 total)
7. `frontend` — Next.js dashboard
8. `prometheus` + `grafana`

**Verify all healthy:**

```bash
docker compose ps
```

**Tear down:**

```bash
docker compose down           # keep volumes
docker compose down -v        # delete volumes too
```

---

## Option B — Native / Nix (step-by-step)

Every terminal must be inside the Nix dev shell:

```bash
# Option 1 — direnv (auto-activates on cd)
direnv allow

# Option 2 — explicit
nix develop
```

**Verify environment:**

```bash
kafka-topics.sh --version        # Kafka 4.x
spark-submit --version           # Spark 4.0.x
schema-registry-start --help     # Confluent Schema Registry
go version                       # go1.26.x
cargo --version                  # cargo 1.x
```

---

### Step 1 — Kafka (KRaft mode)

**Terminal 1**

```bash
cd /home/arqam/WhiteChristmas
nix develop -c bash scripts/kafka-start.sh
```

Expected tail:

```
✅ Kafka ready!
   Topics:
   alerts-arrests
   alerts-crimes
   alerts-sex-offenders
   alerts-violence
   anomaly-alerts
   dead-letter
   raw-events-arrests
   raw-events-crimes
   raw-events-sex-offenders
   raw-events-violence
```

**Verify:**

```bash
nc -z localhost 9092 && echo "Kafka OK"
kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

### Step 2 — Schema Registry

**Terminal 2**

```bash
cd /home/arqam/WhiteChristmas
nix develop -c bash scripts/schema-registry-start.sh
```

Expected output:

```
✅ Schema Registry ready!
   API:  http://localhost:8082
```

**Verify:**

```bash
curl -s http://localhost:8082/subjects
# [] — empty before producer runs
```

---

### Step 3 — PostgreSQL + MongoDB

Start both databases (or use the Docker services for just the DBs):

```bash
# Docker only for DBs, native for everything else
docker compose up -d postgres mongodb
```

Or if you have them installed natively:

```bash
pg_ctl start -D ~/.local/share/postgresql/data
mongod --dbpath ~/.local/share/mongodb/data --fork --logpath /tmp/mongod.log
```

---

### Step 4 — Go WebSocket API

**Terminal 3**

```bash
cd /home/arqam/WhiteChristmas/src/api

# Build (skip if binary is current)
go build -o api .

# Run — ALERTS_TOPICS is comma-separated, one goroutine per topic
KAFKA_BROKERS=localhost:9092 \
ALERTS_TOPICS=alerts-crimes,alerts-violence,alerts-arrests,alerts-sex-offenders \
POSTGRES_CONN_STRING=postgresql://whitechristmas:whitechristmas@localhost:5432/whitechristmas \
MONGODB_CONN_STRING=mongodb://whitechristmas:whitechristmas@localhost:27017/whitechristmas?authSource=admin \
API_PORT=8081 \
  ./api
```

Expected startup:

```
╔════════════════════════════════════════════════════╗
║  🚀 WhiteChristmas Event API                      ║
╚════════════════════════════════════════════════════╝
📊 Kafka Brokers:   localhost:9092
📖 Alert Topics:    alerts-crimes, alerts-violence, alerts-arrests, alerts-sex-offenders
🐘 PostgreSQL: connected
🍃 MongoDB:    connected
✅ API Server Started!
```

**Verify:**

```bash
curl -s http://localhost:8081/health
# {"status":"ok","name":"WhiteChristmas Event API","version":"0.1.0"}

curl -s http://localhost:8081/stats
# {"connected_clients":0,"total_events":0,"timestamp":...}
```

---

### Step 5 — Next.js Dashboard

**Terminal 4**

```bash
cd /home/arqam/WhiteChristmas/src/frontend
bun install        # or: npm install
bun run dev        # or: npm run dev
```

Open **http://localhost:3000**.

---

### Step 6 — Spark StreamProcessor

**Terminal 5** — consumes `raw-events-.*`, writes to `alerts-{dataset}` and `anomaly-alerts`.

```bash
cd /home/arqam/WhiteChristmas/src/spark-jobs

KAFKA_BROKERS=localhost:9092 \
STREAM_INPUT_PATTERN=raw-events-.* \
ANOMALY_TOPIC=anomaly-alerts \
POSTGRES_CONN_STRING=postgresql://whitechristmas:whitechristmas@localhost:5432/whitechristmas \
MONGODB_CONN_STRING=mongodb://whitechristmas:whitechristmas@localhost:27017/whitechristmas?authSource=admin \
AVRO_SCHEMA_PATH=../schemas/crime-event.avsc \
  sbt "runMain com.whitechristmas.spark.StreamProcessor"
```

Expected startup:

```
╔════════════════════════════════════════════════════╗
║  🎯 WhiteChristmas Spark Stream Processor         ║
╚════════════════════════════════════════════════════╝
📊 Kafka Brokers:   localhost:9092
📖 Input Pattern:   raw-events-.*
🚨 Anomaly Topic:   anomaly-alerts
✓ Avro schema loaded
✅ All streams started — awaiting events...
   Main stream:    foreachBatch → PG + Mongo + Kafka alerts
   Metrics stream: windowed district stats → console
   Anomaly stream: z-score detection → Kafka alerts
```

---

### Step 6b — Spark BoltPipeline (optional, parallel path)

**Terminal 6** — Storm-inspired topology: Window Bolt → Anomaly Bolt → Alert Bolt, grouped by `source_dataset + district`.

```bash
cd /home/arqam/WhiteChristmas/src/spark-jobs

KAFKA_BROKERS=localhost:9092 \
BOLT_INPUT_PATTERN=raw-events-.* \
ANOMALY_THRESHOLD=50 \
WINDOW_DURATION_MINUTES=5 \
SLIDE_INTERVAL_MINUTES=1 \
WATERMARK_MINUTES=10 \
POSTGRES_CONN_STRING=postgresql://whitechristmas:whitechristmas@localhost:5432/whitechristmas \
MONGODB_CONN_STRING=mongodb://whitechristmas:whitechristmas@localhost:27017/whitechristmas?authSource=admin \
AVRO_SCHEMA_PATH=../schemas/crime-event.avsc \
  sbt "runMain com.whitechristmas.spark.BoltPipeline"
```

Expected startup:

```
╔══════════════════════════════════════════════════════════╗
║  WhiteChristmas Bolt Pipeline                           ║
║  Parse → District → Window → Anomaly → Alert           ║
╚══════════════════════════════════════════════════════════╝
  Kafka    : localhost:9092  pattern=raw-events-.*
  Window   : 5 minutes, slide=1 minutes
  Threshold: 50 events/window per dataset+district
All bolts active — subscribed to pattern: raw-events-.*
```

---

### Step 7 — Rust Producer

**Terminal 7** — build once, then run. Defaults cover everything.

```bash
cd /home/arqam/WhiteChristmas/src/kafka-producer
cargo build --release
```

#### Production run (default)

```bash
./target/release/producer \
  --kafka-brokers localhost:9092 \
  --schema-registry http://localhost:8082
```

That's it. By default the producer:

- Reads all CSVs from `../../data` automatically
- Spawns one parallel tokio task per dataset (crimes, violence, arrests, sex-offenders)
- Routes each to its own `raw-events-{dataset}` topic
- Loops each CSV forever — restarts from row 1 when exhausted
- Runs at 10 events/sec per dataset (~40 total)

#### Override rate

```bash
./target/release/producer \
  --kafka-brokers localhost:9092 \
  --schema-registry http://localhost:8082 \
  --events-per-sec 20
```

#### Stop after N events per dataset (then restart loop)

```bash
./target/release/producer \
  --kafka-brokers localhost:9092 \
  --schema-registry http://localhost:8082 \
  --max-per-dataset 500
```

#### Single file

```bash
./target/release/producer \
  --csv-path ../../data/Crimes_-_2001_to_Present_20260505.csv \
  --kafka-brokers localhost:9092 \
  --schema-registry http://localhost:8082
```

#### One-event smoke test

```bash
./target/release/producer \
  --csv-path ../../data/Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings_20260505.csv \
  --kafka-brokers localhost:9092 \
  --schema-registry http://localhost:8082 \
  --max-per-dataset 1 \
  --verbose
```

### Dataset → `source_dataset` field → alert topic

| Dataset CSV keyword | `source_dataset` value | Alert topic            | Severity field used                    | 5              | 4                       | 3              | 2               | 1     |
| ------------------- | ---------------------- | ---------------------- | -------------------------------------- | -------------- | ----------------------- | -------------- | --------------- | ----- |
| `violence`          | `violence`             | `alerts-violence`      | GUNSHOT_INJURY + VICTIMIZATION_PRIMARY | fatal/homicide | gunshot                 | head/chest     | leg/arm         | other |
| `crimes`            | `crimes`               | `alerts-crimes`        | Primary Type                           | HOMICIDE       | ASSAULT/BATTERY/ROBBERY | THEFT/BURGLARY | FRAUD/NARCOTICS | other |
| `arrests`           | `arrests`              | `alerts-arrests`       | CHARGE 1 CLASS                         | Class X        | Class 1–2               | Class 3–4      | Misdemeanor A   | other |
| `sex_offenders`     | `sex-offenders`        | `alerts-sex-offenders` | VICTIM MINOR                           | MINOR=Y        | —                       | MINOR=N        | —               | —     |

---

## Observing a Single Event Through Every Stage

### Stage 1 — Schema Registry (schema registered per topic)

After the producer first runs:

```bash
curl -s http://localhost:8082/subjects
# ["raw-events-crimes-value","raw-events-violence-value",
#  "raw-events-arrests-value","raw-events-sex-offenders-value"]

# Inspect the crimes schema
curl -s http://localhost:8082/subjects/raw-events-crimes-value/versions/latest \
  | python3 -m json.tool
```

Decode a live Avro message:

```bash
kafka-avro-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic raw-events-crimes \
  --from-beginning \
  --max-messages 1 \
  --property schema.registry.url=http://localhost:8082
```

### Stage 2 — Kafka raw-events topics (Avro bytes)

```bash
# Verify Confluent wire header: 00 00 00 00 <schema_id>
kafka-console-consumer.sh \
  --topic raw-events-crimes \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 1 | xxd | head -5
```

Consume from all four in parallel:

```bash
for t in crimes violence arrests sex-offenders; do
  kafka-console-consumer.sh \
    --topic "raw-events-$t" \
    --bootstrap-server localhost:9092 \
    --from-beginning --max-messages 1 &
done
wait
```

### Stage 3 — Spark (Terminal 5 / 6)

Within a few seconds the Spark terminal prints a batch table:

```
[Batch 1] Processing 12 events
  ✓ PostgreSQL: persisted 12 events
  ✓ MongoDB: persisted 8 alerts
  ✓ Kafka alerts: routed 8 events to per-dataset topics
```

Events with `severity >= 3` are forwarded to `alerts-{source_dataset}`.
The z-score anomaly stream writes to `anomaly-alerts`.

### Stage 4 — Kafka alert topics (JSON)

```bash
# One topic at a time
kafka-console-consumer.sh \
  --topic alerts-crimes \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 1

# All four alert topics simultaneously
for t in crimes violence arrests sex-offenders; do
  echo "=== alerts-$t ===" && \
  kafka-console-consumer.sh \
    --topic "alerts-$t" \
    --bootstrap-server localhost:9092 \
    --from-beginning --max-messages 1 &
done
wait
```

Expected (one line of JSON per message):

```json
{
  "event_id": "<uuid>",
  "source_dataset": "violence",
  "victim_id": "JO-123456",
  "incident_date": "05/02/2026",
  "district": "011",
  "injury_type": "GUNSHOT",
  "event_label": "GUNSHOT",
  "severity": 4,
  "processed_timestamp": 1234567890000,
  "processed_timestamp_api": 1234567891234
}
```

### Stage 5 — Go API (Terminal 3)

```
[alerts-violence] 📨 Event #7 broadcast: abc-uuid (severity: 4, dataset: violence)
```

```bash
curl -s http://localhost:8081/stats
# {"connected_clients":1,"total_events":7,"timestamp":...}

# Per-dataset breakdown
curl -s http://localhost:8081/datasets/summary

# Recent alerts filtered by dataset
curl -s "http://localhost:8081/alerts/recent?dataset=violence&limit=5"

# Event history filtered by dataset
curl -s "http://localhost:8081/events/history?dataset=crimes&limit=20"
```

### Stage 6 — Next.js Dashboard (browser)

Open **http://localhost:3000**:

- Total Events counter increments in real time
- Event cards show: Event ID, dataset badge, date, district, severity (1–2 green, 3 yellow, 4–5 red)
- Time Replay scrubber → drag left to load PostgreSQL history; click **↩ Live** to resume

---

## Observability (Prometheus + Grafana)

```bash
# Terminal 8 — Prometheus
nix develop -c bash scripts/prometheus-start.sh
# UI: http://localhost:9090

# Terminal 9 — Grafana
nix develop -c bash scripts/grafana-start.sh
# UI: http://localhost:3001  (anonymous admin, no login)
```

**Dashboard panels:**

| Panel                       | Query                                                               |
| --------------------------- | ------------------------------------------------------------------- |
| Total Events Ingested       | `sum(whitechristmas_events_total)`                                  |
| Kafka Messages Consumed     | `sum(whitechristmas_kafka_messages_total)`                          |
| Event Rate — by Severity    | `sum by(severity) (rate(whitechristmas_events_total[1m]))`          |
| Event Rate — by Dataset     | `sum by(dataset) (rate(whitechristmas_events_total[1m]))`           |
| Kafka Alert Rate — by Topic | `sum by(topic) (rate(whitechristmas_kafka_messages_total[1m]))`     |
| HTTP P99 Latency            | `histogram_quantile(0.99, sum by(route,le) (rate(..._bucket[1m])))` |

---

## Batch Analytics (7.1 – 7.6)

Reads CSV files directly and writes aggregated results to PostgreSQL. No Kafka involved.

```bash
cd /home/arqam/WhiteChristmas/src/spark-jobs

# sbt (recommended for dev)
POSTGRES_CONN_STRING=postgresql://whitechristmas:whitechristmas@localhost:5432/whitechristmas \
DATA_DIR=../../data \
KMEANS_K=10 \
MIN_CRIMES_FOR_RATE=100 \
  sbt "runMain com.whitechristmas.spark.BatchAnalytics"

# Assembled JAR via spark-submit
sbt assembly
spark-submit \
  --class com.whitechristmas.spark.BatchAnalytics \
  target/scala-2.13/whitechristmas-spark-assembly-0.1.0.jar
```

Tables written:

| Section | Table(s)                                              |
| ------- | ----------------------------------------------------- |
| 7.1     | `crime_trends`                                        |
| 7.2     | `arrest_rate_analysis`, `top_arrest_rate_crime_types` |
| 7.3     | `violence_analysis`, `top_violence_community`         |
| 7.4     | `sex_offender_proximity`, `district_offender_density` |
| 7.5     | `hotspots`                                            |
| 7.6     | `correlations`                                        |

REST endpoints (populated after batch job):

```bash
curl -s http://localhost:8081/analytics/crime-trends
curl -s http://localhost:8081/analytics/hotspots
curl -s "http://localhost:8081/analytics/correlations?type=district"
curl -s http://localhost:8081/analytics/arrest-rates
curl -s http://localhost:8081/analytics/violence
```

---

## Teardown (Native)

```bash
# Stop producer, Spark, API, Next.js — Ctrl+C in each terminal

nix develop -c bash scripts/grafana-stop.sh
nix develop -c bash scripts/prometheus-stop.sh
nix develop -c bash scripts/schema-registry-stop.sh
nix develop -c bash scripts/kafka-stop.sh
```

---

## Quick Diagnostics

| Symptom                              | Check                                                                                                    |
| ------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| Producer can't reach Schema Registry | `curl http://localhost:8082/subjects`                                                                    |
| Producer can't reach Kafka           | `nc -z localhost 9092`                                                                                   |
| Spark sees no messages               | `kafka-console-consumer.sh --topic raw-events-crimes --bootstrap-server localhost:9092 --from-beginning` |
| Alert topics empty                   | Check Spark terminal — `severity < 3` events are not forwarded                                           |
| API not consuming alerts             | `curl http://localhost:8081/stats` — `total_events` stuck at 0                                           |
| Dashboard not updating               | Browser DevTools → Network → WS → `ws://localhost:8081/ws`                                               |
| Wrong alert topic routing            | Verify `source_dataset` values: `crimes`, `violence`, `arrests`, `sex-offenders`                         |

**Log files (native):**

```
.local/logs/kafka.log
.local/logs/schema-registry.log
.local/logs/prometheus.log
.local/logs/grafana.log
```

**Log files (Docker):**

```bash
docker compose logs -f kafka
docker compose logs -f spark-streaming
docker compose logs -f api
docker compose logs -f producer
```

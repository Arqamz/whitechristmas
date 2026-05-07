use anyhow::{Context, Result};
use apache_avro::{to_avro_datum, types::Value as AvroValue, Schema};
use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Rust Kafka producer — CSV → Avro → Schema Registry → Kafka
#[derive(Parser, Debug)]
#[command(name = "WhiteChristmas Producer")]
#[command(about = "Stream crime events from CSV to Kafka with Avro serialization")]
struct Args {
    /// Path to CSV data file
    #[arg(short, long)]
    csv_path: PathBuf,

    /// Kafka bootstrap servers
    #[arg(short, long, default_value = "localhost:9092")]
    kafka_brokers: String,

    /// Kafka topic to publish to
    #[arg(short, long, default_value = "raw-events")]
    topic: String,

    /// Schema Registry URL
    #[arg(short, long, default_value = "http://localhost:8081")]
    schema_registry: String,

    /// Events per second rate limit
    #[arg(short, long, default_value = "10")]
    events_per_sec: u32,

    /// Maximum events to publish (0 = all)
    #[arg(short, long, default_value = "0")]
    max_events: usize,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

/// CSV row from Violence_Reduction dataset
#[derive(Debug, Deserialize)]
struct CrimeRecord {
    #[serde(rename = "Date")]
    date: Option<String>,
    #[serde(rename = "Time")]
    time: Option<String>,
    #[serde(rename = "Victim's Name")]
    victim_name: Option<String>,
    #[serde(rename = "Body Part Injured")]
    injury_type: Option<String>,
    #[serde(rename = "District")]
    district: Option<String>,
    #[serde(rename = "Location Description")]
    location: Option<String>,
}

fn calculate_severity(injury_type: &Option<String>) -> i32 {
    match injury_type.as_deref() {
        Some(s) => {
            let lower = s.to_lowercase();
            if lower.contains("fatality") || lower.contains("fatal") || lower.contains("homicide") {
                5
            } else if lower.contains("shooting") || lower.contains("gunshot") {
                4
            } else if lower.contains("head") || lower.contains("brain") || lower.contains("chest") {
                3
            } else if lower.contains("leg") || lower.contains("arm") {
                2
            } else {
                1
            }
        }
        None => 1,
    }
}

/// Register schema with Confluent Schema Registry and return schema ID.
/// Uses the TopicNameStrategy subject: `{topic}-value`.
async fn register_schema(registry_url: &str, topic: &str, schema_json: &str) -> Result<u32> {
    let subject = format!("{}-value", topic);
    let url = format!("{}/subjects/{}/versions", registry_url, subject);

    let body = serde_json::json!({ "schema": schema_json });

    let resp = reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .json(&body)
        .send()
        .await
        .context("Failed to reach Schema Registry")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("Schema Registry returned {}: {}", status, text);
    }

    let json: serde_json::Value = resp.json().await?;
    let id = json["id"]
        .as_u64()
        .context("Schema Registry response missing 'id' field")? as u32;

    Ok(id)
}

/// Serialize an Avro record and prepend the Confluent wire format header:
/// [magic byte 0x00] [schema_id: 4 bytes big-endian] [avro binary]
fn confluent_encode(schema: &Schema, record: AvroValue, schema_id: u32) -> Result<Vec<u8>> {
    let avro_bytes = to_avro_datum(schema, record).context("Failed to serialize Avro record")?;

    let mut payload = Vec::with_capacity(5 + avro_bytes.len());
    payload.push(0x00); // Confluent magic byte
    payload.extend_from_slice(&schema_id.to_be_bytes());
    payload.extend_from_slice(&avro_bytes);
    Ok(payload)
}

/// Convert an optional String into an Avro union(null | string) value.
fn opt_string(v: Option<String>) -> AvroValue {
    match v {
        Some(s) if !s.is_empty() => AvroValue::Union(1, Box::new(AvroValue::String(s))),
        _ => AvroValue::Union(0, Box::new(AvroValue::Null)),
    }
}

/// Convert an optional i32 into an Avro union(null | int) value.
fn opt_int(v: Option<i32>) -> AvroValue {
    match v {
        Some(i) => AvroValue::Union(1, Box::new(AvroValue::Int(i))),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_max_level(if args.verbose {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        })
        .init();

    info!("╔════════════════════════════════════════════════════╗");
    info!("║  🦀 WhiteChristmas Kafka Producer (Rust + Avro)  ║");
    info!("╚════════════════════════════════════════════════════╝");
    info!("CSV Path:        {}", args.csv_path.display());
    info!("Kafka Brokers:   {}", args.kafka_brokers);
    info!("Topic:           {}", args.topic);
    info!("Schema Registry: {}", args.schema_registry);
    info!("Rate Limit:      {}/sec", args.events_per_sec);

    if !args.csv_path.exists() {
        error!("❌ CSV file not found: {}", args.csv_path.display());
        anyhow::bail!("CSV file not found");
    }

    // Load Avro schema from the schemas directory relative to this binary's crate root
    let schema_path = std::env::current_dir()
        .unwrap_or_default()
        .join("../schemas/crime-event.avsc");

    let schema_json = std::fs::read_to_string(&schema_path)
        .with_context(|| format!("Cannot read schema file: {}", schema_path.display()))?;

    let schema = Schema::parse_str(&schema_json).context("Failed to parse Avro schema")?;
    info!("✓ Avro schema loaded");

    // Register schema with Schema Registry
    info!(
        "Registering schema with Schema Registry at {}...",
        args.schema_registry
    );
    let schema_id = register_schema(&args.schema_registry, &args.topic, &schema_json)
        .await
        .context("Schema registration failed")?;
    info!("✓ Schema registered (id={})", schema_id);

    // Create Kafka producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &args.kafka_brokers)
        .set("message.timeout.ms", "5000")
        .set("acks", "1")
        .create()
        .context("Failed to create Kafka producer")?;
    info!("✓ Kafka producer initialized");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let delay_ms = 1000 / args.events_per_sec as u64;
    let mut event_count: usize = 0;
    let mut published: usize = 0;

    let file = std::fs::File::open(&args.csv_path).context("Failed to open CSV")?;
    let mut reader = csv::Reader::from_reader(file);

    info!("📖 Streaming CSV → Avro → Kafka...");

    for result in reader.deserialize() {
        if args.max_events > 0 && event_count >= args.max_events {
            info!("✓ Reached max event limit");
            break;
        }
        event_count += 1;

        let record: CrimeRecord = match result {
            Ok(r) => r,
            Err(e) => {
                warn!("Skipping malformed row {}: {}", event_count, e);
                continue;
            }
        };

        let event_id = Uuid::new_v4().to_string();
        let severity = calculate_severity(&record.injury_type);
        let processed_ts = chrono::Utc::now().timestamp_millis();

        // Build Avro record matching crime-event.avsc
        let avro_record = AvroValue::Record(vec![
            ("event_id".into(), AvroValue::String(event_id.clone())),
            ("victim_id".into(), opt_string(record.victim_name)),
            (
                "incident_date".into(),
                AvroValue::String(
                    record
                        .date
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| "unknown".into()),
                ),
            ),
            ("incident_time".into(), opt_string(record.time)),
            ("location".into(), opt_string(record.location)),
            ("district".into(), opt_string(record.district)),
            ("injury_type".into(), opt_string(record.injury_type)),
            ("severity".into(), opt_int(Some(severity))),
            ("processed_timestamp".into(), AvroValue::Long(processed_ts)),
        ]);

        let payload = match confluent_encode(&schema, avro_record, schema_id) {
            Ok(b) => b,
            Err(e) => {
                warn!("Avro encode failed for event {}: {}", event_id, e);
                continue;
            }
        };

        let kafka_record = FutureRecord::to(&args.topic)
            .key(event_id.as_bytes())
            .payload(&payload);

        match producer.send(kafka_record, Duration::from_secs(5)).await {
            Ok(_) => {
                published += 1;
                if published <= 10 || published % 100 == 0 {
                    info!(
                        "📨 Published #{}: {} (severity={})",
                        published,
                        &event_id[..8],
                        severity
                    );
                }
            }
            Err((e, _)) => warn!("Kafka send failed: {}", e),
        }

        sleep(Duration::from_millis(delay_ms)).await;
    }

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!(
        "✅ Done! Read: {} rows, Published: {} events",
        event_count, published
    );

    producer.flush(Duration::from_secs(10));
    Ok(())
}

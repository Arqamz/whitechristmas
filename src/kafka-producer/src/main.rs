use anyhow::{Context, Result};
use clap::Parser;
use rdkafka::client::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Rust Kafka producer for crime event streaming
#[derive(Parser, Debug)]
#[command(name = "WhiteChristmas Producer")]
#[command(about = "Stream crime events from CSV to Kafka")]
struct Args {
    /// Path to CSV data file
    #[arg(short, long, default_value = "/data/Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings_20260505.csv")]
    csv_path: PathBuf,

    /// Kafka bootstrap servers
    #[arg(short, long, default_value = "localhost:9092")]
    kafka_brokers: String,

    /// Kafka topic to publish to
    #[arg(short, long, default_value = "raw-events")]
    topic: String,

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

/// Crime event from CSV
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    #[serde(rename = "Community Area")]
    community_area: Option<String>,
    #[serde(rename = "Location Description")]
    location: Option<String>,
    #[serde(rename = "Latitude")]
    latitude: Option<String>,
    #[serde(rename = "Longitude")]
    longitude: Option<String>,
    // Additional fields from CSV that might exist
    #[serde(flatten)]
    extra: serde_json::Value,
}

/// Structured crime event for Kafka
#[derive(Debug, Clone, Serialize)]
struct CrimeEvent {
    event_id: String,
    victim_id: Option<String>,
    incident_date: String,
    incident_time: Option<String>,
    location: Option<String>,
    district: Option<String>,
    injury_type: Option<String>,
    severity: Option<u8>,
    processed_timestamp: i64,
}

impl CrimeEvent {
    /// Convert CSV record to structured event
    fn from_csv_record(record: CrimeRecord) -> Self {
        let severity = calculate_severity(&record.injury_type);
        let processed_timestamp = chrono::Utc::now().timestamp_millis();

        Self {
            event_id: Uuid::new_v4().to_string(),
            victim_id: record.victim_name.filter(|s| !s.is_empty()),
            incident_date: record.date.unwrap_or_else(|| "unknown".to_string()),
            incident_time: record.time.filter(|s| !s.is_empty()),
            location: record.location.filter(|s| !s.is_empty()),
            district: record.district.filter(|s| !s.is_empty()),
            injury_type: record.injury_type.filter(|s| !s.is_empty()),
            severity,
            processed_timestamp,
        }
    }
}

/// Calculate severity from injury type
fn calculate_severity(injury_type: &Option<String>) -> Option<u8> {
    match injury_type.as_deref() {
        Some(s) => {
            let lower = s.to_lowercase();
            Some(match lower.as_str() {
                s if s.contains("fatality") || s.contains("fatal") || s.contains("death") => 5,
                s if s.contains("homicide") => 5,
                s if s.contains("shooting") => 4,
                s if s.contains("gunshot") => 4,
                s if s.contains("head") || s.contains("brain") => 4,
                s if s.contains("chest") || s.contains("torso") => 3,
                s if s.contains("leg") || s.contains("arm") => 2,
                s if s.contains("other") => 1,
                _ => 2, // Default medium severity
            })
        }
        None => Some(1), // Default low severity
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(if args.verbose {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        })
        .init();

    info!("🚀 WhiteChristmas Kafka Producer");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("CSV Path: {}", args.csv_path.display());
    info!("Kafka Brokers: {}", args.kafka_brokers);
    info!("Topic: {}", args.topic);
    info!("Rate Limit: {}/sec", args.events_per_sec);
    if args.max_events > 0 {
        info!("Max Events: {}", args.max_events);
    }
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("");

    // Verify CSV file exists
    if !args.csv_path.exists() {
        error!("CSV file not found: {}", args.csv_path.display());
        return Err(anyhow::anyhow!("CSV file not found"));
    }

    // Create Kafka producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &args.kafka_brokers)
        .set("message.timeout.ms", "5000")
        .set("acks", "1")
        .create()
        .context("Failed to create Kafka producer")?;

    info!("✓ Kafka producer initialized");
    info!("");

    // Read and stream CSV
    let delay_ms = 1000 / args.events_per_sec as u64;
    let mut event_count = 0;
    let mut published_count = 0;

    let file = std::fs::File::open(&args.csv_path)
        .context("Failed to open CSV file")?;

    let mut reader = csv::Reader::from_reader(file);

    info!("📖 Reading CSV and streaming to Kafka...");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    for result in reader.deserialize() {
        if args.max_events > 0 && event_count >= args.max_events {
            info!("✓ Reached max event limit");
            break;
        }

        event_count += 1;

        match result {
            Ok(record) => {
                let crime_record: CrimeRecord = record;
                let event = CrimeEvent::from_csv_record(crime_record);

                // Serialize to JSON
                let payload = serde_json::to_string(&event)?;

                // Create Kafka record
                let record = FutureRecord::to(&args.topic)
                    .key(&event.event_id)
                    .payload(&payload);

                // Send to Kafka
                match producer.send(record, Duration::from_secs(5)).await {
                    Ok(_) => {
                        published_count += 1;
                        if published_count % 100 == 0 || published_count < 10 {
                            info!("📨 Published event #{}: {}", published_count, event.event_id);
                        }
                    }
                    Err((e, _)) => {
                        warn!("Failed to publish event: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("Failed to deserialize CSV record: {}", e);
            }
        }

        // Rate limiting
        if event_count % 100 == 0 {
            info!("  → Processing event #{}, published: #{}", event_count, published_count);
        }

        sleep(Duration::from_millis(delay_ms)).await;
    }

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("✅ Publishing complete!");
    info!("   Total records read: {}", event_count);
    info!("   Successfully published: {}", published_count);
    info!("");

    // Flush to ensure all messages are sent
    producer.flush(Duration::from_secs(10));

    Ok(())
}

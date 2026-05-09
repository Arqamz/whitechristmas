use anyhow::{Context, Result};
use apache_avro::{to_avro_datum, types::Value as AvroValue, Schema};
use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::Deserialize;
use std::fs::File;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

// ── YAML config (for --json-mode) ─────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
struct YamlConfig {
    kafka: Option<KafkaYamlConfig>,
}

#[derive(Debug, Deserialize, Default)]
struct KafkaYamlConfig {
    brokers: Option<String>,
    crime_topic: Option<String>,
    publication_rate: Option<u32>,
}

// ── CLI args ───────────────────────────────────────────────────────────────

/// WhiteChristmas Kafka producer — all five Chicago public safety datasets → Avro → Kafka
#[derive(Parser, Debug)]
#[command(name = "WhiteChristmas Producer")]
#[command(
    about = "Stream all five Chicago public safety datasets to Kafka with Avro serialization"
)]
struct Args {
    /// Path to a single CSV file (auto-detects dataset type from filename)
    #[arg(long, conflicts_with = "csv_dir")]
    csv_path: Option<PathBuf>,

    /// Directory containing Chicago CSV files (streams all recognised datasets)
    #[arg(long, conflicts_with = "csv_path")]
    csv_dir: Option<PathBuf>,

    /// Kafka bootstrap servers
    #[arg(long, default_value = "localhost:9092")]
    kafka_brokers: String,

    /// Kafka topic
    #[arg(long, default_value = "raw-events")]
    topic: String,

    /// Schema Registry URL
    #[arg(long, default_value = "http://localhost:8081")]
    schema_registry: String,

    /// Events per second (across all datasets combined)
    #[arg(long, default_value = "10")]
    events_per_sec: u32,

    /// Maximum events per dataset (0 = unlimited)
    #[arg(long, default_value = "0")]
    max_per_dataset: usize,

    /// Maximum total events across all datasets (0 = unlimited)
    #[arg(long, default_value = "0")]
    max_events: usize,

    /// Path to config.yaml — used in --json-mode for topic and rate defaults
    #[arg(long)]
    config: Option<PathBuf>,

    /// Stream Crime CSV as JSON to crime-events topic (no Avro, no Schema Registry)
    #[arg(long, default_value = "false")]
    json_mode: bool,

    /// Verbose logging
    #[arg(short, long)]
    verbose: bool,
}

// ── Dataset type ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum DatasetType {
    ViolenceReduction,
    Crimes,
    Arrests,
    SexOffenders,
    PoliceStations, // reference data — not streamed
    Unknown,
}

impl DatasetType {
    fn from_filename(path: &PathBuf) -> Self {
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();
        if name.contains("violence") {
            DatasetType::ViolenceReduction
        } else if name.contains("crimes") {
            DatasetType::Crimes
        } else if name.contains("arrests") {
            DatasetType::Arrests
        } else if name.contains("sex_offenders") || name.contains("sex offenders") {
            DatasetType::SexOffenders
        } else if name.contains("police_stations") || name.contains("police stations") {
            DatasetType::PoliceStations
        } else {
            DatasetType::Unknown
        }
    }

    fn label(&self) -> &'static str {
        match self {
            DatasetType::ViolenceReduction => "violence_reduction",
            DatasetType::Crimes => "crimes",
            DatasetType::Arrests => "arrests",
            DatasetType::SexOffenders => "sex_offenders",
            DatasetType::PoliceStations => "police_stations",
            DatasetType::Unknown => "unknown",
        }
    }
}

// ── Per-dataset CSV row types ──────────────────────────────────────────────

/// Violence Reduction — Victims of Homicides and Non-Fatal Shootings
#[derive(Debug, Deserialize)]
struct ViolenceRow {
    #[serde(rename = "CASE_NUMBER")]
    case_number: Option<String>,
    #[serde(rename = "DATE")]
    date: Option<String>,
    #[serde(rename = "HOUR")]
    hour: Option<String>,
    #[serde(rename = "BLOCK")]
    block: Option<String>,
    #[serde(rename = "DISTRICT")]
    district: Option<String>,
    #[serde(rename = "BEAT")]
    beat: Option<String>,
    #[serde(rename = "LOCATION_DESCRIPTION")]
    location_description: Option<String>,
    #[serde(rename = "VICTIMIZATION_PRIMARY")]
    victimization_primary: Option<String>,
    #[serde(rename = "INCIDENT_PRIMARY")]
    incident_primary: Option<String>,
    #[serde(rename = "GUNSHOT_INJURY_I")]
    gunshot_injury: Option<String>,
    #[serde(rename = "LATITUDE")]
    latitude: Option<String>,
    #[serde(rename = "LONGITUDE")]
    longitude: Option<String>,
}

/// Crimes — 2001 to Present
#[derive(Debug, Deserialize)]
struct CrimeRow {
    #[serde(rename = "Case Number")]
    case_number: Option<String>,
    #[serde(rename = "Date")]
    date: Option<String>,
    #[serde(rename = "Block")]
    block: Option<String>,
    #[serde(rename = "Primary Type")]
    primary_type: Option<String>,
    #[serde(rename = "Description")]
    description: Option<String>,
    #[serde(rename = "Location Description")]
    location_description: Option<String>,
    #[serde(rename = "Arrest")]
    arrest: Option<String>,
    #[serde(rename = "Beat")]
    beat: Option<String>,
    #[serde(rename = "District")]
    district: Option<String>,
    #[serde(rename = "Latitude")]
    latitude: Option<String>,
    #[serde(rename = "Longitude")]
    longitude: Option<String>,
}

/// Arrests
#[derive(Debug, Deserialize)]
struct ArrestRow {
    #[serde(rename = "CB_NO")]
    cb_no: Option<String>,
    #[serde(rename = "CASE NUMBER")]
    case_number: Option<String>,
    #[serde(rename = "ARREST DATE")]
    arrest_date: Option<String>,
    #[serde(rename = "RACE")]
    race: Option<String>,
    #[serde(rename = "CHARGE 1 DESCRIPTION")]
    charge_description: Option<String>,
    #[serde(rename = "CHARGE 1 TYPE")]
    charge_type: Option<String>,
    #[serde(rename = "CHARGE 1 CLASS")]
    charge_class: Option<String>,
    #[serde(rename = "CHARGES DESCRIPTION")]
    charges_description: Option<String>,
}

/// Sex Offenders
#[derive(Debug, Deserialize)]
struct SexOffenderRow {
    #[serde(rename = "LAST")]
    last: Option<String>,
    #[serde(rename = "FIRST")]
    first: Option<String>,
    #[serde(rename = "BLOCK")]
    block: Option<String>,
    #[serde(rename = "GENDER")]
    gender: Option<String>,
    #[serde(rename = "RACE")]
    race: Option<String>,
    #[serde(rename = "BIRTH DATE")]
    birth_date: Option<String>,
    #[serde(rename = "VICTIM MINOR")]
    victim_minor: Option<String>,
}

// ── Unified normalised event ───────────────────────────────────────────────

struct UnifiedEvent {
    source_dataset: String,
    victim_id: Option<String>,
    incident_date: String,
    incident_time: Option<String>,
    location: Option<String>,
    district: Option<String>,
    beat: Option<String>,
    crime_type: Option<String>,
    injury_type: Option<String>,
    severity: i32,
    latitude: Option<String>,
    longitude: Option<String>,
    is_arrest: Option<bool>,
}

// ── Severity calculators ───────────────────────────────────────────────────

fn severity_violence(gunshot: &Option<String>, primary: &Option<String>) -> i32 {
    // Gunshot = YES always at least 4
    if gunshot.as_deref().map(|s| s.trim().to_uppercase()) == Some("YES".into()) {
        return 4;
    }
    match primary.as_deref().map(str::to_lowercase).as_deref() {
        Some(s) if s.contains("fatality") || s.contains("fatal") || s.contains("homicide") => 5,
        Some(s) if s.contains("shooting") || s.contains("gunshot") => 4,
        Some(s) if s.contains("head") || s.contains("brain") || s.contains("chest") => 3,
        Some(s) if s.contains("leg") || s.contains("arm") => 2,
        _ => 1,
    }
}

fn severity_crime(primary_type: &Option<String>) -> i32 {
    match primary_type.as_deref().map(str::to_uppercase).as_deref() {
        Some("HOMICIDE") => 5,
        Some(
            "ASSAULT"
            | "BATTERY"
            | "ROBBERY"
            | "ARSON"
            | "KIDNAPPING"
            | "CRIMINAL SEXUAL ASSAULT"
            | "HUMAN TRAFFICKING",
        ) => 4,
        Some(
            "THEFT"
            | "BURGLARY"
            | "MOTOR VEHICLE THEFT"
            | "STALKING"
            | "INTIMIDATION"
            | "SEX OFFENSE"
            | "CRIM SEXUAL ASSAULT",
        ) => 3,
        Some(
            "DECEPTIVE PRACTICE"
            | "FRAUD"
            | "FORGERY"
            | "NARCOTICS"
            | "WEAPONS VIOLATION"
            | "CONCEALED CARRY LICENSE VIOLATION",
        ) => 2,
        _ => 1,
    }
}

fn severity_arrest(charge_class: &Option<String>) -> i32 {
    match charge_class
        .as_deref()
        .map(str::trim)
        .map(str::to_uppercase)
        .as_deref()
    {
        Some("X") => 5,
        Some("1") | Some("2") => 4,
        Some("3") | Some("4") => 3,
        Some("A") => 2,
        _ => 1,
    }
}

fn severity_sex_offender(victim_minor: &Option<String>) -> i32 {
    match victim_minor
        .as_deref()
        .map(str::trim)
        .map(str::to_uppercase)
        .as_deref()
    {
        Some("Y") => 5,
        _ => 3,
    }
}

// ── Row → UnifiedEvent ─────────────────────────────────────────────────────

impl From<ViolenceRow> for UnifiedEvent {
    fn from(r: ViolenceRow) -> Self {
        let (date, time) = split_datetime(r.date.as_deref());
        let time = time.or_else(|| r.hour.as_ref().map(|h| format!("{:0>2}:00", h.trim())));
        let sev = severity_violence(&r.gunshot_injury, &r.victimization_primary);
        let crime_type = r
            .victimization_primary
            .clone()
            .or_else(|| r.incident_primary.clone());
        UnifiedEvent {
            source_dataset: "violence_reduction".into(),
            victim_id: r.case_number,
            incident_date: date,
            incident_time: time,
            location: r
                .location_description
                .filter(|s| !s.is_empty())
                .or(r.block.filter(|s| !s.is_empty())),
            district: r.district,
            beat: r.beat,
            crime_type,
            injury_type: r.victimization_primary,
            severity: sev,
            latitude: r.latitude,
            longitude: r.longitude,
            is_arrest: None,
        }
    }
}

impl From<CrimeRow> for UnifiedEvent {
    fn from(r: CrimeRow) -> Self {
        let (date, time) = split_datetime(r.date.as_deref());
        let sev = severity_crime(&r.primary_type);
        let is_arrest = r
            .arrest
            .as_deref()
            .map(|s| matches!(s.trim().to_lowercase().as_str(), "true" | "yes" | "1"));
        UnifiedEvent {
            source_dataset: "crimes".into(),
            victim_id: r.case_number,
            incident_date: date,
            incident_time: time,
            location: r
                .location_description
                .filter(|s| !s.is_empty())
                .or(r.block.filter(|s| !s.is_empty())),
            district: r.district,
            beat: r.beat,
            crime_type: r.primary_type,
            injury_type: r.description,
            severity: sev,
            latitude: r.latitude,
            longitude: r.longitude,
            is_arrest,
        }
    }
}

impl From<ArrestRow> for UnifiedEvent {
    fn from(r: ArrestRow) -> Self {
        let (date, time) = split_datetime(r.arrest_date.as_deref());
        let sev = severity_arrest(&r.charge_class);
        let crime_type = r
            .charge_description
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| r.charges_description.clone().filter(|s| !s.is_empty()));
        let victim_id = r.cb_no.or(r.case_number);
        UnifiedEvent {
            source_dataset: "arrests".into(),
            victim_id,
            incident_date: date,
            incident_time: time,
            location: None,
            district: None,
            beat: None,
            crime_type,
            injury_type: r.charge_type,
            severity: sev,
            latitude: None,
            longitude: None,
            is_arrest: Some(true),
        }
    }
}

impl From<SexOffenderRow> for UnifiedEvent {
    fn from(r: SexOffenderRow) -> Self {
        let sev = severity_sex_offender(&r.victim_minor);
        let victim_id = match (&r.last, &r.first) {
            (Some(l), Some(f)) if !l.is_empty() || !f.is_empty() => {
                Some(format!("{} {}", l.trim(), f.trim()))
            }
            (Some(l), None) => Some(l.clone()),
            _ => None,
        };
        UnifiedEvent {
            source_dataset: "sex_offenders".into(),
            victim_id,
            incident_date: r.birth_date.unwrap_or_else(|| "unknown".into()),
            incident_time: None,
            location: r.block,
            district: None,
            beat: None,
            crime_type: Some("SEX OFFENDER REGISTRATION".into()),
            injury_type: r.victim_minor.map(|v| format!("VICTIM_MINOR:{}", v.trim())),
            severity: sev,
            latitude: None,
            longitude: None,
            is_arrest: None,
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Split "MM/DD/YYYY HH:MM:SS AM/PM" → ("MM/DD/YYYY", Some("HH:MM:SS"))
fn split_datetime(s: Option<&str>) -> (String, Option<String>) {
    match s {
        None | Some("") => ("unknown".into(), None),
        Some(s) => {
            let s = s.trim();
            // Typical Chicago format: "05/02/2026 11:49:00 PM"
            if let Some(space) = s.find(' ') {
                let date = s[..space].to_string();
                let rest = s[space + 1..].trim();
                // drop AM/PM suffix if present
                let time = rest.splitn(2, ' ').next().map(|t| t.to_string());
                (date, time)
            } else {
                (s.to_string(), None)
            }
        }
    }
}

fn opt_str(v: Option<String>) -> AvroValue {
    match v {
        Some(s) if !s.is_empty() => AvroValue::Union(1, Box::new(AvroValue::String(s))),
        _ => AvroValue::Union(0, Box::new(AvroValue::Null)),
    }
}

fn opt_int(v: Option<i32>) -> AvroValue {
    match v {
        Some(i) => AvroValue::Union(1, Box::new(AvroValue::Int(i))),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    }
}

fn opt_bool(v: Option<bool>) -> AvroValue {
    match v {
        Some(b) => AvroValue::Union(1, Box::new(AvroValue::Boolean(b))),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    }
}

// ── Avro / Schema Registry ────────────────────────────────────────────────

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
        .context("Schema Registry response missing 'id'")? as u32;
    Ok(id)
}

fn confluent_encode(schema: &Schema, record: AvroValue, schema_id: u32) -> Result<Vec<u8>> {
    let avro_bytes = to_avro_datum(schema, record).context("Avro serialization failed")?;
    let mut payload = Vec::with_capacity(5 + avro_bytes.len());
    payload.push(0x00);
    payload.extend_from_slice(&schema_id.to_be_bytes());
    payload.extend_from_slice(&avro_bytes);
    Ok(payload)
}

fn to_avro_record(event: UnifiedEvent, schema: &Schema, schema_id: u32) -> Result<Vec<u8>> {
    let record = AvroValue::Record(vec![
        (
            "event_id".into(),
            AvroValue::String(Uuid::new_v4().to_string()),
        ),
        (
            "source_dataset".into(),
            AvroValue::String(event.source_dataset),
        ),
        ("victim_id".into(), opt_str(event.victim_id)),
        (
            "incident_date".into(),
            AvroValue::String(event.incident_date),
        ),
        ("incident_time".into(), opt_str(event.incident_time)),
        ("location".into(), opt_str(event.location)),
        ("district".into(), opt_str(event.district)),
        ("beat".into(), opt_str(event.beat)),
        ("crime_type".into(), opt_str(event.crime_type)),
        ("injury_type".into(), opt_str(event.injury_type)),
        ("severity".into(), opt_int(Some(event.severity))),
        ("latitude".into(), opt_str(event.latitude)),
        ("longitude".into(), opt_str(event.longitude)),
        ("is_arrest".into(), opt_bool(event.is_arrest)),
        (
            "processed_timestamp".into(),
            AvroValue::Long(chrono::Utc::now().timestamp_millis()),
        ),
    ]);
    confluent_encode(schema, record, schema_id)
}

// ── CSV streaming functions ────────────────────────────────────────────────

/// Stream one CSV of a known type, yielding UnifiedEvents.
/// Returns (rows_read, rows_published).
async fn stream_csv(
    path: &PathBuf,
    dataset_type: &DatasetType,
    schema: &Schema,
    schema_id: u32,
    producer: &FutureProducer,
    topic: &str,
    delay_ms: u64,
    max_per_dataset: usize,
    total_published: &mut usize,
    max_total: usize,
) -> Result<(usize, usize)> {
    let file = File::open(path).with_context(|| format!("Cannot open {}", path.display()))?;
    let mut read = 0usize;
    let mut published = 0usize;

    macro_rules! stream {
        ($row_type:ty, $convert:expr) => {{
            let mut rdr = csv::Reader::from_reader(file);
            for result in rdr.deserialize::<$row_type>() {
                if max_per_dataset > 0 && published >= max_per_dataset {
                    break;
                }
                if max_total > 0 && *total_published >= max_total {
                    break;
                }
                read += 1;
                let row: $row_type = match result {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("Skipping malformed row {}: {}", read, e);
                        continue;
                    }
                };
                let event: UnifiedEvent = $convert(row);
                let payload = match to_avro_record(event, schema, schema_id) {
                    Ok(p) => p,
                    Err(e) => {
                        warn!("Avro encode failed row {}: {}", read, e);
                        continue;
                    }
                };
                let kafka_rec = FutureRecord::to(topic).key(b"key").payload(&payload);
                match producer.send(kafka_rec, Duration::from_secs(5)).await {
                    Ok(_) => {
                        published += 1;
                        *total_published += 1;
                        if published <= 5 || published % 500 == 0 {
                            info!(
                                "  [{dataset}] #{published} published (row {read})",
                                dataset = dataset_type.label()
                            );
                        }
                    }
                    Err((e, _)) => warn!("Kafka send failed: {}", e),
                }
                sleep(Duration::from_millis(delay_ms)).await;
            }
        }};
    }

    match dataset_type {
        DatasetType::ViolenceReduction => {
            stream!(ViolenceRow, |r: ViolenceRow| UnifiedEvent::from(r));
        }
        DatasetType::Crimes => {
            stream!(CrimeRow, |r: CrimeRow| UnifiedEvent::from(r));
        }
        DatasetType::Arrests => {
            stream!(ArrestRow, |r: ArrestRow| UnifiedEvent::from(r));
        }
        DatasetType::SexOffenders => {
            stream!(SexOffenderRow, |r: SexOffenderRow| UnifiedEvent::from(r));
        }
        _ => {
            warn!("Skipping unsupported dataset: {}", path.display());
        }
    }

    Ok((read, published))
}

// ── JSON crime simulator ───────────────────────────────────────────────────

/// Stream the Crimes CSV as plain JSON to the crime-events topic.
/// Required fields: case_number, date, block, primary_type, district,
/// arrest, latitude, longitude.  Malformed rows are logged and discarded.
async fn stream_json_crimes(
    path: &PathBuf,
    producer: &FutureProducer,
    topic: &str,
    delay_ms: u64,
    max_events: usize,
) -> Result<(usize, usize)> {
    let file = File::open(path).with_context(|| format!("Cannot open {}", path.display()))?;
    let mut rdr = csv::Reader::from_reader(file);
    let mut published = 0usize;
    let mut malformed = 0usize;

    for (row_num, result) in rdr.deserialize::<CrimeRow>().enumerate() {
        if max_events > 0 && published >= max_events {
            break;
        }

        let row: CrimeRow = match result {
            Ok(r) => r,
            Err(e) => {
                warn!("Row {} malformed ({}), discarded", row_num + 1, e);
                malformed += 1;
                continue;
            }
        };

        // Validate all 8 required fields — discard if any are blank
        macro_rules! require {
            ($opt:expr, $name:expr) => {
                match $opt.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                    Some(v) => v.to_string(),
                    None => {
                        warn!("Row {} missing '{}', discarded", row_num + 1, $name);
                        malformed += 1;
                        continue;
                    }
                }
            };
        }

        let case_number = require!(row.case_number, "case_number");
        let date = require!(row.date, "date");
        let block = require!(row.block, "block");
        let primary_type = require!(row.primary_type, "primary_type");
        let district = require!(row.district, "district");
        let arrest = require!(row.arrest, "arrest");
        let latitude = require!(row.latitude, "latitude");
        let longitude = require!(row.longitude, "longitude");

        let msg = serde_json::json!({
            "case_number":  case_number,
            "date":         date,
            "block":        block,
            "primary_type": primary_type,
            "district":     district,
            "arrest":       arrest,
            "latitude":     latitude,
            "longitude":    longitude,
        });

        let payload = msg.to_string();
        let rec = FutureRecord::to(topic)
            .key(case_number.as_bytes())
            .payload(payload.as_bytes());

        match producer.send(rec, Duration::from_secs(5)).await {
            Ok(_) => {
                published += 1;
                if published <= 5 || published % 500 == 0 {
                    info!("[JSON] #{published}  case={case_number}  type={primary_type}  district={district}");
                }
            }
            Err((e, _)) => warn!("Kafka send failed: {}", e),
        }

        sleep(Duration::from_millis(delay_ms)).await;
    }

    Ok((published + malformed, published))
}

// ── main ──────────────────────────────────────────────────────────────────

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

    // ── Load config.yaml if provided (used for json_mode defaults) ───────
    let mut yaml_brokers: Option<String> = None;
    let mut yaml_topic: Option<String> = None;
    let mut yaml_rate: Option<u32> = None;

    if let Some(cfg_path) = &args.config {
        let cfg_str = std::fs::read_to_string(cfg_path)
            .with_context(|| format!("Cannot read config: {}", cfg_path.display()))?;
        let cfg: YamlConfig =
            serde_yaml::from_str(&cfg_str).context("Invalid YAML in config file")?;
        if let Some(k) = cfg.kafka {
            yaml_brokers = k.brokers;
            yaml_topic = k.crime_topic;
            yaml_rate = k.publication_rate;
        }
    }

    // CLI args take precedence; config.yaml fills unset values
    let kafka_brokers = if args.kafka_brokers != "localhost:9092" {
        args.kafka_brokers.clone()
    } else {
        yaml_brokers.unwrap_or(args.kafka_brokers.clone())
    };

    let events_per_sec = yaml_rate.unwrap_or(args.events_per_sec);

    // ── JSON mode — crime simulator ───────────────────────────────────────
    if args.json_mode {
        let topic = yaml_topic.unwrap_or_else(|| "crime-events".to_string());
        // Override with --topic if explicitly set
        let topic = if args.topic != "raw-events" {
            args.topic.clone()
        } else {
            topic
        };

        info!("╔══════════════════════════════════════════════════════════╗");
        info!("║  🦀 WhiteChristmas Crime Simulator — JSON Mode          ║");
        info!("╚══════════════════════════════════════════════════════════╝");
        info!("Kafka        : {}", kafka_brokers);
        info!("Topic        : {}", topic);
        info!("Rate         : {}/sec", events_per_sec);

        let crimes_path = if let Some(p) = &args.csv_path {
            p.clone()
        } else if let Some(dir) = &args.csv_dir {
            std::fs::read_dir(dir)?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .find(|p| DatasetType::from_filename(p) == DatasetType::Crimes)
                .ok_or_else(|| anyhow::anyhow!("No Crimes CSV found in {:?}", dir))?
        } else {
            anyhow::bail!("Provide --csv-path or --csv-dir");
        };

        info!("CSV          : {}", crimes_path.display());

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &kafka_brokers)
            .set("message.timeout.ms", "5000")
            .set("acks", "1")
            .create()
            .context("Failed to create Kafka producer")?;

        let delay_ms = 1000u64 / events_per_sec as u64;
        let (read, published) =
            stream_json_crimes(&crimes_path, &producer, &topic, delay_ms, args.max_events).await?;

        producer.flush(Duration::from_secs(10));
        info!("Done — read: {read}, published: {published}");
        return Ok(());
    }

    info!("╔══════════════════════════════════════════════════════════╗");
    info!("║  🦀 WhiteChristmas Kafka Producer — All Five Datasets  ║");
    info!("╚══════════════════════════════════════════════════════════╝");

    // Collect CSV paths to process
    let csv_files: Vec<PathBuf> = if let Some(dir) = &args.csv_dir {
        if !dir.is_dir() {
            error!("--csv-dir is not a directory: {}", dir.display());
            anyhow::bail!("Not a directory");
        }
        let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e.eq_ignore_ascii_case("csv"))
                    .unwrap_or(false)
            })
            .filter(|p| DatasetType::from_filename(p) != DatasetType::PoliceStations)
            .filter(|p| DatasetType::from_filename(p) != DatasetType::Unknown)
            .collect();
        files.sort();
        files
    } else if let Some(path) = &args.csv_path {
        vec![path.clone()]
    } else {
        error!("Provide either --csv-path or --csv-dir");
        anyhow::bail!("No input specified");
    };

    if csv_files.is_empty() {
        error!("No recognised CSV files found");
        anyhow::bail!("Empty dataset list");
    }

    info!("Datasets to stream ({}):", csv_files.len());
    for p in &csv_files {
        info!(
            "  {:25} → {}",
            DatasetType::from_filename(p).label(),
            p.file_name().unwrap_or_default().to_string_lossy()
        );
    }
    info!("Kafka:           {}", kafka_brokers);
    info!("Topic:           {}", args.topic);
    info!("Schema Registry: {}", args.schema_registry);
    info!("Rate:            {}/sec", events_per_sec);
    if args.max_per_dataset > 0 {
        info!("Max/dataset:     {}", args.max_per_dataset);
    }
    if args.max_events > 0 {
        info!("Max total:       {}", args.max_events);
    }

    // Load Avro schema
    let schema_path = std::env::current_dir()
        .unwrap_or_default()
        .join("../schemas/crime-event.avsc");
    let schema_json = std::fs::read_to_string(&schema_path)
        .with_context(|| format!("Cannot read schema: {}", schema_path.display()))?;
    let schema = Schema::parse_str(&schema_json).context("Invalid Avro schema")?;
    info!("✓ Avro schema loaded ({} fields)", {
        // quick field count
        schema_json.matches("\"name\"").count() - 1 // subtract the record name itself
    });

    // Register schema
    let schema_id = register_schema(&args.schema_registry, &args.topic, &schema_json)
        .await
        .context("Schema registration failed")?;
    info!("✓ Schema registered with Registry (id={})", schema_id);

    // Kafka producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        .set("message.timeout.ms", "5000")
        .set("acks", "1")
        .create()
        .context("Failed to create Kafka producer")?;
    info!("✓ Kafka producer ready");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let delay_ms = 1000u64 / events_per_sec as u64;
    let mut total_published = 0usize;
    let mut grand_total_read = 0usize;

    for path in &csv_files {
        let dataset_type = DatasetType::from_filename(path);
        info!(
            "\n▶ Streaming [{}] {}",
            dataset_type.label(),
            path.file_name().unwrap_or_default().to_string_lossy()
        );

        if args.max_events > 0 && total_published >= args.max_events {
            info!("✓ Global max_events reached — stopping");
            break;
        }

        let (read, published) = stream_csv(
            path,
            &dataset_type,
            &schema,
            schema_id,
            &producer,
            &args.topic,
            delay_ms,
            args.max_per_dataset,
            &mut total_published,
            args.max_events,
        )
        .await?;

        grand_total_read += read;
        info!(
            "  ✓ [{dataset}] done — read: {read}, published: {published}",
            dataset = dataset_type.label()
        );
    }

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!(
        "✅ All datasets complete — total read: {}, published: {}",
        grand_total_read, total_published
    );

    producer.flush(Duration::from_secs(10));
    Ok(())
}

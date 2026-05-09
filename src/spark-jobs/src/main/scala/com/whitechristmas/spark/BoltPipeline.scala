package com.whitechristmas.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.bson.Document
import com.mongodb.client.MongoClients
import scala.jdk.CollectionConverters._

/** WhiteChristmas Bolt Pipeline — Storm-inspired Spark Structured Streaming.
  *
  * Reads JSON crime events from the `crime-events` Kafka topic (published by
  * the Rust producer in --json-mode) and implements a Storm bolt topology:
  *
  * Kafka (crime-events JSON) └─► Parse Bolt validates required fields;
  * malformed → logged + discarded └─► District Bolt tags tuples with police
  * district + event timestamp └─► Window Bolt sliding count window per district
  * └─► Anomaly Bolt emits when window count exceeds threshold └─► Alert Bolt
  * persists to PostgreSQL (alerts) + MongoDB (alert_logs)
  *
  * Run: sbt "runMain com.whitechristmas.spark.BoltPipeline"
  *
  * Environment variables (all have sensible defaults): KAFKA_BROKERS (default:
  * localhost:9092) CRIME_TOPIC (default: crime-events) WINDOW_DURATION_MINUTES
  * (default: 5) SLIDE_INTERVAL_MINUTES (default: 1) ANOMALY_THRESHOLD (default:
  * 50) WATERMARK_MINUTES (default: 10) POSTGRES_CONN_STRING
  * postgresql://user:pass@host:port/db MONGODB_CONN_STRING mongodb+srv://...
  */
object BoltPipeline {

  private def getConfig(key: String, default: String = ""): String =
    sys.props.getOrElse(key, sys.env.getOrElse(key, default))

  private def parsePostgresUrl(connStr: String): (String, String, String) = {
    val s = connStr.stripPrefix("postgresql://").stripPrefix("postgres://")
    val atIdx = s.lastIndexOf('@')
    val up = s.substring(0, atIdx)
    val hostDb = s.substring(atIdx + 1)
    val ci = up.indexOf(':')
    (s"jdbc:postgresql://$hostDb", up.substring(0, ci), up.substring(ci + 1))
  }

  /** JSON schema for events published by the Rust producer in --json-mode. */
  private val crimeSchema: StructType = StructType(
    Array(
      StructField("case_number", StringType, nullable = true),
      StructField("date", StringType, nullable = true),
      StructField("block", StringType, nullable = true),
      StructField("primary_type", StringType, nullable = true),
      StructField("district", StringType, nullable = true),
      StructField("arrest", StringType, nullable = true),
      StructField("latitude", StringType, nullable = true),
      StructField("longitude", StringType, nullable = true)
    )
  )

  def main(args: Array[String]): Unit = {

    val kafkaBrokers = getConfig("KAFKA_BROKERS", "localhost:9092")
    val crimeTopic = getConfig("CRIME_TOPIC", "crime-events")
    val windowDuration = getConfig("WINDOW_DURATION_MINUTES", "5") + " minutes"
    val slideInterval = getConfig("SLIDE_INTERVAL_MINUTES", "1") + " minutes"
    val threshold = getConfig("ANOMALY_THRESHOLD", "50").toInt
    val watermark = getConfig("WATERMARK_MINUTES", "10") + " minutes"
    val checkpointDir = "/tmp/spark-checkpoint/bolt-pipeline"

    val pgConnStr = getConfig("POSTGRES_CONN_STRING")
    val mongoConnStr = getConfig("MONGODB_CONN_STRING")
    val pgEnabled = pgConnStr.nonEmpty
    val mongoEnabled = mongoConnStr.nonEmpty

    val (pgJdbcUrl, pgUser, pgPass) =
      if (pgEnabled) parsePostgresUrl(pgConnStr)
      else ("", "", "")

    val spark = SparkSession
      .builder()
      .appName("WhiteChristmas-BoltPipeline")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("╔══════════════════════════════════════════════════════════╗")
    println("║  WhiteChristmas Bolt Pipeline                           ║")
    println("║  Parse → District → Window → Anomaly → Alert           ║")
    println("╚══════════════════════════════════════════════════════════╝")
    println(s"  Kafka    : $kafkaBrokers  topic=$crimeTopic")
    println(s"  Window   : $windowDuration, slide=$slideInterval")
    println(s"  Threshold: $threshold crimes/window")
    println(s"  PostgreSQL: ${if (pgEnabled) "enabled" else "DISABLED"}")
    println(s"  MongoDB  : ${if (mongoEnabled) "enabled" else "DISABLED"}")
    println()

    // ── Kafka source ──────────────────────────────────────────────────────
    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", crimeTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    // ════════════════════════════════════════════════════════════════════════
    // Parse Bolt
    // Deserialise JSON value from Kafka. Tuples missing required fields
    // are logged to the malformed-events console sink and discarded; they
    // are NOT forwarded to the District Bolt.
    // ════════════════════════════════════════════════════════════════════════
    val parsedRaw = raw
      .select(
        col("timestamp").as("kafka_ts"),
        from_json(col("value").cast("string"), crimeSchema).as("d")
      )
      .select(col("kafka_ts"), col("d.*"))

    val requiredPresent =
      col("case_number").isNotNull && col("case_number") =!= "" &&
        col("date").isNotNull && col("date") =!= "" &&
        col("primary_type").isNotNull && col("primary_type") =!= "" &&
        col("district").isNotNull && col("district") =!= ""

    val validEvents = parsedRaw.filter(requiredPresent)
    val malformedEvents = parsedRaw.filter(!requiredPresent)

    // Log malformed tuples to console — do not forward downstream
    val malformedQuery = malformedEvents.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .queryName("parse-bolt-malformed-logger")
      .start()

    // ════════════════════════════════════════════════════════════════════════
    // District Bolt
    // Tags each valid tuple with its police district and parses the event
    // timestamp for downstream time-based windowing.
    // ════════════════════════════════════════════════════════════════════════
    val districtTagged = validEvents
      .withColumn(
        "event_time",
        coalesce(
          to_timestamp(col("date"), "MM/dd/yyyy hh:mm:ss a"),
          col("kafka_ts") // fallback to Kafka ingestion time
        )
      )
      .withWatermark("event_time", watermark)

    // ════════════════════════════════════════════════════════════════════════
    // Window Bolt  (Sliding Window)
    // Groups district-tagged tuples into overlapping time windows and emits
    // per-district crime counts at each slide interval.
    // ════════════════════════════════════════════════════════════════════════
    val windowedCounts = districtTagged
      .groupBy(
        window(col("event_time"), windowDuration, slideInterval)
          .as("time_window"),
        col("district")
      )
      .agg(
        count("*").as("event_count"),
        first("kafka_ts").as("window_first_event"),
        last("kafka_ts").as("window_last_event")
      )
      .select(
        col("district"),
        col("time_window.start").as("window_start"),
        col("time_window.end").as("window_end"),
        col("event_count"),
        col("window_first_event"),
        col("window_last_event")
      )

    // ════════════════════════════════════════════════════════════════════════
    // Anomaly Bolt
    // Compares each district's window count against the configurable threshold.
    // Emits an anomaly tuple — including district ID, window count, threshold,
    // and timestamp — when the count is exceeded.
    // ════════════════════════════════════════════════════════════════════════
    val anomalies = windowedCounts
      .filter(col("event_count") > threshold)
      .withColumn("threshold", lit(threshold))
      .withColumn("detected_at", current_timestamp())
      .withColumn(
        "alert_severity",
        when(col("event_count") > threshold * 3, "CRITICAL")
          .when(col("event_count") > threshold * 2, "HIGH")
          .otherwise("MEDIUM")
      )

    // ════════════════════════════════════════════════════════════════════════
    // Alert Bolt
    // Consumes anomaly tuples and persists structured alert records to:
    //   PostgreSQL → alerts table
    //   MongoDB    → whitechristmas.alert_logs collection
    // Each record includes: district, window_start/end, event_count,
    // threshold, detected_at, alert_severity.
    // ════════════════════════════════════════════════════════════════════════
    val alertQuery = anomalies.writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          batchDF.cache()
          val count = batchDF.count()
          println(s"\n[Alert Bolt] Batch $batchId — $count anomalies detected")
          batchDF.show(5, truncate = false)

          // ── PostgreSQL sink ─────────────────────────────────────────────
          if (pgEnabled) {
            try {
              batchDF.write
                .mode("append")
                .format("jdbc")
                .option("url", pgJdbcUrl)
                .option("dbtable", "alerts")
                .option("driver", "org.postgresql.Driver")
                .option("user", pgUser)
                .option("password", pgPass)
                .option("stringtype", "unspecified")
                .save()
              println(s"  ✓ PostgreSQL: wrote $count alert records")
            } catch {
              case e: Exception =>
                println(s"  ✗ PostgreSQL write failed: ${e.getMessage}")
            }
          }

          // ── MongoDB sink ────────────────────────────────────────────────
          if (mongoEnabled) {
            try {
              val docs = batchDF.toJSON.collect().map(Document.parse)
              val client = MongoClients.create(mongoConnStr)
              try {
                client
                  .getDatabase("whitechristmas")
                  .getCollection("alert_logs")
                  .insertMany(docs.toList.asJava)
                println(s"  ✓ MongoDB: wrote $count alert_logs documents")
              } finally {
                client.close()
              }
            } catch {
              case e: Exception =>
                println(s"  ✗ MongoDB write failed: ${e.getMessage}")
            }
          }

          batchDF.unpersist()
        }
      }
      .option("checkpointLocation", s"$checkpointDir/alert-bolt")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .queryName("alert-bolt")
      .start()

    println("All bolts active — awaiting crime events on topic: " + crimeTopic)
    println()
    println("  Parse Bolt    — JSON deserialise + required-field validation")
    println("  District Bolt — tag by police district, parse event timestamp")
    println(
      s"  Window Bolt   — $windowDuration sliding window, $slideInterval slide"
    )
    println(s"  Anomaly Bolt  — threshold = $threshold crimes / window")
    println("  Alert Bolt    — PostgreSQL alerts + MongoDB alert_logs")
    println()
    println("Press Ctrl+C to stop")

    spark.streams.awaitAnyTermination()
  }
}

package com.whitechristmas.spark

import org.apache.spark.sql._
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.bson.Document
import com.mongodb.client.MongoClients
import scala.jdk.CollectionConverters._
import scala.io.Source

/** WhiteChristmas Bolt Pipeline — Storm-inspired Spark Structured Streaming.
  *
  * Reads Avro CrimeEvents from all raw-events-* topics and implements a
  * sliding-window volume anomaly detector grouped by source_dataset + district:
  *
  * raw-events-crimes ┐ raw-events-violence ├─► Parse Bolt → District Bolt →
  * Window Bolt raw-events-arrests │ → Anomaly Bolt (threshold breach)
  * raw-events-sex-offenders ┘ → Alert Bolt → PostgreSQL alerts → MongoDB
  * alert_logs → Kafka anomaly-alerts
  *
  * Complementary to StreamProcessor, NOT redundant: StreamProcessor — per-event
  * routing (severity >= 3) → alerts-{dataset} BoltPipeline — sustained volume
  * surges → anomaly-alerts
  *
  * Called standalone via main(), or as part of the combined Pipeline entry
  * point via setup() which registers all streams without blocking.
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

  /** Register all BoltPipeline streaming queries against the given session.
    * Does NOT call awaitAnyTermination — callers are responsible for that. Safe
    * to call when a SparkSession already exists (getOrCreate is a no-op).
    */
  def setup(spark: SparkSession): Unit = {

    val kafkaBrokers = getConfig("KAFKA_BROKERS", "localhost:9092")
    val inputTopicPattern = getConfig("BOLT_INPUT_PATTERN", "raw-events-.*")
    val anomalyTopic = getConfig("ANOMALY_TOPIC", "anomaly-alerts")
    val windowDuration = getConfig("WINDOW_DURATION_MINUTES", "5") + " minutes"
    val slideInterval = getConfig("SLIDE_INTERVAL_MINUTES", "1") + " minutes"
    val threshold = getConfig("ANOMALY_THRESHOLD", "50").toInt
    val watermark = getConfig("WATERMARK_MINUTES", "10") + " minutes"
    val checkpointDir = "/tmp/spark-checkpoint/bolt-pipeline"
    val schemaPath =
      getConfig("AVRO_SCHEMA_PATH", "../schemas/crime-event.avsc")

    val pgConnStr = getConfig("POSTGRES_CONN_STRING")
    val mongoConnStr = getConfig("MONGODB_CONN_STRING")
    val pgEnabled = pgConnStr.nonEmpty
    val mongoEnabled = mongoConnStr.nonEmpty

    val (pgJdbcUrl, pgUser, pgPass) =
      if (pgEnabled) parsePostgresUrl(pgConnStr) else ("", "", "")

    println("╔══════════════════════════════════════════════════════════╗")
    println("║  ⚡ BoltPipeline — volume-threshold anomaly detector    ║")
    println("╚══════════════════════════════════════════════════════════╝")
    println(s"  Pattern  : $inputTopicPattern")
    println(s"  Window   : $windowDuration, slide=$slideInterval")
    println(s"  Threshold: $threshold events/window per dataset+district")
    println(
      s"  Output   : PostgreSQL alerts + MongoDB alert_logs + $anomalyTopic"
    )
    println()

    val avroSchemaStr = Source.fromFile(schemaPath).mkString
    println("✓ Avro schema loaded (BoltPipeline)")

    val stripConfluentHeader = udf((bytes: Array[Byte]) =>
      if (bytes != null && bytes.length > 5) bytes.slice(5, bytes.length)
      else bytes
    )

    // ── Kafka source ──────────────────────────────────────────────────────────
    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribePattern", inputTopicPattern)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .option(
        "kafka.group.id",
        "whitechristmas-bolt-pipeline"
      ) // distinct from StreamProcessor
      .load()

    // ── Parse Bolt ────────────────────────────────────────────────────────────
    val parsedRaw = raw
      .select(
        col("timestamp").as("kafka_ts"),
        stripConfluentHeader(col("value")).as("avro_payload")
      )
      .select(
        col("kafka_ts"),
        from_avro(col("avro_payload"), avroSchemaStr).as("event")
      )
      .select(
        col("kafka_ts"),
        col("event.event_id"),
        col("event.source_dataset"),
        col("event.victim_id"),
        col("event.incident_date"),
        col("event.incident_time"),
        col("event.location"),
        col("event.district"),
        col("event.beat"),
        col("event.crime_type"),
        col("event.injury_type"),
        col("event.severity"),
        col("event.latitude"),
        col("event.longitude"),
        col("event.is_arrest"),
        col("event.processed_timestamp")
      )

    val requiredPresent =
      col("incident_date").isNotNull && col("incident_date") =!= "" &&
        col("district").isNotNull && col("district") =!= ""

    val validEvents = parsedRaw.filter(requiredPresent)
    val malformedEvents = parsedRaw.filter(!requiredPresent)

    malformedEvents.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .queryName("bolt-parse-malformed")
      .start()

    // ── District Bolt ─────────────────────────────────────────────────────────
    val districtTagged = validEvents
      .withColumn(
        "event_time",
        coalesce(
          to_timestamp(
            concat(col("incident_date"), lit(" "), col("incident_time")),
            "MM/dd/yyyy HH:mm:ss"
          ),
          to_timestamp(col("incident_date"), "MM/dd/yyyy"),
          col("kafka_ts").cast("timestamp")
        )
      )
      .withWatermark("event_time", watermark)

    // ── Window Bolt — per source_dataset + district ───────────────────────────
    val windowedCounts = districtTagged
      .groupBy(
        window(col("event_time"), windowDuration, slideInterval)
          .as("time_window"),
        col("source_dataset"),
        col("district")
      )
      .agg(
        count("*").as("event_count"),
        first("kafka_ts").as("window_first_event"),
        last("kafka_ts").as("window_last_event")
      )
      .select(
        col("source_dataset"),
        col("district"),
        col("time_window.start").as("window_start"),
        col("time_window.end").as("window_end"),
        col("event_count"),
        col("window_first_event"),
        col("window_last_event")
      )

    // ── Anomaly Bolt ──────────────────────────────────────────────────────────
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

    // ── Alert Bolt ────────────────────────────────────────────────────────────
    // Writes to PostgreSQL alerts + MongoDB alert_logs + anomaly-alerts topic.
    // Does NOT write to alerts-{dataset} — those are reserved for StreamProcessor's
    // per-event CrimeEvent JSON. Volume anomalies go to anomaly-alerts.
    anomalies.writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          batchDF.cache()
          val cnt = batchDF.count()
          println(
            s"\n[Alert Bolt Batch $batchId] $cnt volume anomalies detected"
          )
          batchDF.show(5, truncate = false)

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
              println(s"  ✓ PostgreSQL alerts: $cnt rows")
            } catch {
              case e: Exception =>
                println(s"  ✗ PostgreSQL write failed: ${e.getMessage}")
            }
          }

          if (mongoEnabled) {
            try {
              val docs = batchDF.toJSON.collect().map(Document.parse)
              val client = MongoClients.create(mongoConnStr)
              try {
                client
                  .getDatabase("whitechristmas")
                  .getCollection("alert_logs")
                  .insertMany(docs.toList.asJava)
                println(s"  ✓ MongoDB alert_logs: $cnt docs")
              } finally { client.close() }
            } catch {
              case e: Exception =>
                println(s"  ✗ MongoDB write failed: ${e.getMessage}")
            }
          }

          // Volume anomaly alerts → anomaly-alerts topic (NOT alerts-{dataset}).
          // Schema: { alert_type, source_dataset, district, window_start, window_end,
          //           event_count, threshold, detected_at, alert_severity }
          try {
            batchDF
              .select(
                col("district").as("key"),
                to_json(
                  struct(
                    lit("volume_threshold").as("alert_type"),
                    col("source_dataset"),
                    col("district"),
                    col("window_start"),
                    col("window_end"),
                    col("event_count"),
                    col("threshold"),
                    col("detected_at"),
                    col("alert_severity")
                  )
                ).as("value")
              )
              .write
              .format("kafka")
              .option("kafka.bootstrap.servers", kafkaBrokers)
              .option("topic", anomalyTopic)
              .save()
            println(s"  ✓ Kafka $anomalyTopic: $cnt volume anomalies published")
          } catch {
            case e: Exception =>
              println(s"  ✗ Kafka write failed: ${e.getMessage}")
          }

          batchDF.unpersist()
        }
        ()
      }
      .option("checkpointLocation", s"$checkpointDir/alert-bolt")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .queryName("bolt-alert")
      .start()

    println()
    println("  BoltPipeline streams active:")
    println(
      s"    window  → $windowDuration sliding, $slideInterval slide, per dataset+district"
    )
    println(s"    anomaly → threshold=$threshold events/window → $anomalyTopic")
    println()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("WhiteChristmas-BoltPipeline")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    setup(spark)
    spark.streams.awaitAnyTermination()
  }
}

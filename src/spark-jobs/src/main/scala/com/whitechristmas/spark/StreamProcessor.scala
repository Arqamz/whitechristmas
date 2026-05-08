package com.whitechristmas.spark

import org.apache.spark.sql._
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.{avg, col, count, current_timestamp, lit, max, round, stddev, struct, sum, to_json, unix_millis, when, window}
import org.apache.spark.sql.types._
import org.bson.Document
import com.mongodb.client.MongoClients
import scala.jdk.CollectionConverters._
import scala.io.Source

/**
 * Spark Structured Streaming job for WhiteChristmas pipeline.
 *
 * Flow: Kafka raw-events (Avro + Confluent wire format)
 *         → strip 5-byte header → from_avro → enrich
 *         → foreachBatch: PostgreSQL (all events) + MongoDB alerts + Kafka alerts topic
 *
 * Confluent wire format: [0x00][schema_id: 4 bytes BE][avro binary payload]
 */
object StreamProcessor {

  /**
   * Read config from JVM system property first (set by sbt javaOptions / -D flags),
   * then fall back to OS environment variable, then to a default value.
   */
  private def getConfig(key: String, default: String = ""): String =
    sys.props.getOrElse(key, sys.env.getOrElse(key, default))

  /**
   * Parse a PostgreSQL URL into JDBC components.
   *   Input:  postgresql://user:pass@host:port/db
   *   Output: (jdbcUrl, user, password)
   *
   * lastIndexOf('@') handles passwords that contain '@'.
   * indexOf(':')     handles passwords that contain ':' (splits on first colon only).
   */
  private def parsePostgresUrl(connStr: String): (String, String, String) = {
    val withoutScheme = connStr.stripPrefix("postgresql://").stripPrefix("postgres://")
    val atIdx         = withoutScheme.lastIndexOf('@')
    val userPass      = withoutScheme.substring(0, atIdx)
    val hostPortDb    = withoutScheme.substring(atIdx + 1)
    val colonIdx      = userPass.indexOf(':')
    val user          = userPass.substring(0, colonIdx)
    val pass          = userPass.substring(colonIdx + 1)
    (s"jdbc:postgresql://$hostPortDb", user, pass)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("WhiteChristmas-StreamProcessor")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("╔════════════════════════════════════════════════════╗")
    println("║  🎯 WhiteChristmas Spark Stream Processor         ║")
    println("╚════════════════════════════════════════════════════╝")
    println()

    val kafkaBrokers   = getConfig("KAFKA_BROKERS", "localhost:9092")
    val inputTopic     = "raw-events"
    val alertsTopic    = "alerts"
    val checkpointDir  = "/tmp/spark-checkpoint"
    val schemaPath     = getConfig("AVRO_SCHEMA_PATH", "../schemas/crime-event.avsc")

    val pgConnString    = getConfig("POSTGRES_CONN_STRING")
    val mongoConnString = getConfig("MONGODB_CONN_STRING")
    val pgEnabled       = pgConnString.nonEmpty
    val mongoEnabled    = mongoConnString.nonEmpty

    println(s"📊 Kafka Brokers:  $kafkaBrokers")
    println(s"📖 Input Topic:    $inputTopic")
    println(s"📤 Output Topic:   $alertsTopic")
    println(s"📋 Avro Schema:    $schemaPath")
    println(s"🐘 PostgreSQL:     ${if (pgEnabled) "enabled" else "disabled (POSTGRES_CONN_STRING not set)"}")
    println(s"🍃 MongoDB:        ${if (mongoEnabled) "enabled" else "disabled (MONGODB_CONN_STRING not set)"}")
    println()

    val (pgJdbcUrl, pgUser, pgPass) =
      if (pgEnabled) parsePostgresUrl(pgConnString)
      else ("", "", "")

    val avroSchemaStr = Source.fromFile(schemaPath).mkString
    println("✓ Avro schema loaded")

    // UDF: strip the 5-byte Confluent wire format header [magic(1) + schemaId(4)]
    val stripConfluentHeader = udf((bytes: Array[Byte]) =>
      if (bytes != null && bytes.length > 5) bytes.slice(5, bytes.length)
      else bytes
    )

    try {
      println("📥 Connecting to Kafka...")

      val raw = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBrokers)
        .option("subscribe", inputTopic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()

      // Strip Confluent header, deserialize Avro, flatten to top-level columns
      val parsed = raw
        .select(
          col("timestamp").as("kafka_timestamp"),
          stripConfluentHeader(col("value")).as("avro_payload")
        )
        .select(
          col("kafka_timestamp"),
          from_avro(col("avro_payload"), avroSchemaStr).as("event")
        )
        .select(
          col("kafka_timestamp"),
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

      // Enrich with server-side timestamps, alert flag, and effective crime label
      val enriched = parsed
        .withColumn("processed_at", current_timestamp())
        .withColumn(
          "received_lag_ms",
          (col("processed_at").cast("long") - col("kafka_timestamp").cast("long")) * 1000
        )
        .withColumn("is_alert", col("severity") >= 3)
        // Unified label: prefer crime_type, fall back to injury_type
        .withColumn(
          "event_label",
          when(col("crime_type").isNotNull && col("crime_type") =!= "", col("crime_type"))
            .otherwise(col("injury_type"))
        )

      // ── Main query: single foreachBatch fans out to all sinks ──────────────
      println("🚀 Starting main foreachBatch stream (PostgreSQL + MongoDB + Kafka)...")

      val mainQuery = enriched
        .writeStream
        .outputMode("append")
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          if (!batchDF.isEmpty) {
            batchDF.cache()
            val totalCount = batchDF.count()
            println(s"\n[Batch $batchId] Processing $totalCount events")
            batchDF.show(5, truncate = false)

            // 1. Write ALL events to PostgreSQL
            if (pgEnabled) {
              try {
                batchDF.write
                  .mode("append")
                  .format("jdbc")
                  .option("url", pgJdbcUrl)
                  .option("dbtable", "events")
                  .option("driver", "org.postgresql.Driver")
                  .option("user", pgUser)
                  .option("password", pgPass)
                  .option("stringtype", "unspecified")
                  .save()
                println(s"  ✓ PostgreSQL: persisted $totalCount events")
              } catch {
                case e: Exception =>
                  println(s"  ✗ PostgreSQL write failed: ${e.getMessage}")
              }
            }

            // Filter for high-severity alerts (severity >= 3)
            val alertsBatch = batchDF.filter(col("severity") >= 3).cache()

            if (!alertsBatch.isEmpty) {
              val alertCount = alertsBatch.count()

              // 2. Write alerts to MongoDB
              if (mongoEnabled) {
                try {
                  val docs   = alertsBatch.toJSON.collect().map(Document.parse)
                  val client = MongoClients.create(mongoConnString)
                  try {
                    client
                      .getDatabase("whitechristmas")
                      .getCollection("alerts")
                      .insertMany(docs.toList.asJava)
                    println(s"  ✓ MongoDB: persisted $alertCount alerts")
                  } finally {
                    client.close()
                  }
                } catch {
                  case e: Exception =>
                    println(s"  ✗ MongoDB write failed: ${e.getMessage}")
                }
              }

              // 3. Forward alerts to Kafka `alerts` topic as JSON
              try {
                alertsBatch
                  .select(
                    col("event_id").as("key"),
                    to_json(struct(
                      col("event_id"),
                      col("source_dataset"),
                      col("victim_id"),
                      col("incident_date"),
                      col("incident_time"),
                      col("location"),
                      col("district"),
                      col("beat"),
                      col("crime_type"),
                      col("injury_type"),
                      col("event_label"),
                      col("severity"),
                      col("latitude"),
                      col("longitude"),
                      col("is_arrest"),
                      col("processed_timestamp"),
                      unix_millis(col("processed_at")).as("processed_timestamp_api")
                    )).as("value")
                  )
                  .write
                  .format("kafka")
                  .option("kafka.bootstrap.servers", kafkaBrokers)
                  .option("topic", alertsTopic)
                  .save()
                println(s"  ✓ Kafka alerts: forwarded $alertCount events")
              } catch {
                case e: Exception =>
                  println(s"  ✗ Kafka alerts write failed: ${e.getMessage}")
              }
            }

            alertsBatch.unpersist()
            batchDF.unpersist()
          }
        }
        .option("checkpointLocation", s"$checkpointDir/main")
        .start()

      // ── Metrics query: 10-second windowed severity stats per district ────────
      println("📊 Starting district metrics stream (console)...")

      val metricsQuery = enriched
        .groupBy(
          window(col("processed_at"), "10 seconds"),
          col("district")
        )
        .agg(
          count("event_id").as("event_count"),
          avg("severity").as("avg_severity"),
          sum(when(col("severity") >= 4, 1).otherwise(0)).as("critical_count")
        )
        .writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", s"$checkpointDir/metrics")
        .start()

      // ── Anomaly detection: rolling z-score per district (2-minute window) ───
      // Strategy: for each 2-minute tumbling window, compute mean and stddev of
      // severity per district. Flag events as anomalous when their severity
      // deviates more than 2 standard deviations from the window mean.
      println("🔬 Starting z-score anomaly detection stream...")

      // Compute per-district severity statistics over a 2-minute sliding window
      val windowedStats = enriched
        .withWatermark("processed_at", "30 seconds")
        .groupBy(
          window(col("processed_at"), "2 minutes", "30 seconds"),
          col("district")
        )
        .agg(
          count("event_id").as("window_count"),
          avg("severity").as("mean_severity"),
          stddev("severity").as("stddev_severity"),
          max("severity").as("max_severity")
        )

      // Join back to enriched stream to compute per-event z-score
      val anomalyQuery = windowedStats
        .writeStream
        .outputMode("update")
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          if (!batchDF.isEmpty) {
            // Flag districts where max_severity exceeds mean + 2*stddev (z-score > 2)
            val anomalies = batchDF
              .withColumn(
                "z_score",
                when(
                  col("stddev_severity").isNotNull && col("stddev_severity") > 0,
                  (col("max_severity") - col("mean_severity")) / col("stddev_severity")
                ).otherwise(lit(0.0))
              )
              .filter(col("z_score") > 2.0 || (col("stddev_severity").isNull && col("max_severity") >= 4))

            if (!anomalies.isEmpty) {
              println(s"\n[Anomaly Batch $batchId] 🚨 Statistical anomalies detected:")
              anomalies.select(
                col("district"),
                col("window.start").as("window_start"),
                col("window_count"),
                round(col("mean_severity"), 2).as("mean_sev"),
                round(col("stddev_severity"), 2).as("stddev_sev"),
                col("max_severity"),
                round(col("z_score"), 2).as("z_score")
              ).show(10, truncate = false)

              // Forward anomaly alerts to Kafka as a separate signal
              if (kafkaBrokers.nonEmpty) {
                try {
                  anomalies
                    .select(
                      col("district").as("key"),
                      to_json(struct(
                        lit("anomaly").as("alert_type"),
                        col("district"),
                        col("window.start").as("window_start"),
                        col("window_count"),
                        round(col("mean_severity"), 2).as("mean_severity"),
                        round(col("stddev_severity"), 2).as("stddev_severity"),
                        col("max_severity"),
                        round(col("z_score"), 2).as("z_score"),
                        lit(System.currentTimeMillis()).as("detected_at")
                      )).as("value")
                    )
                    .write
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaBrokers)
                    .option("topic", alertsTopic)
                    .save()
                  println(s"  ✓ Anomaly alerts forwarded to Kafka topic: $alertsTopic")
                } catch {
                  case e: Exception =>
                    println(s"  ✗ Anomaly Kafka write failed: ${e.getMessage}")
                }
              }
            }
          }
        }
        .option("checkpointLocation", s"$checkpointDir/anomaly")
        .start()

      println()
      println("═══════════════════════════════════════════════════")
      println("✅ All streams started — awaiting events...")
      println("   Main stream:    foreachBatch → PG + Mongo + Kafka alerts")
      println("   Metrics stream: windowed district stats → console")
      println("   Anomaly stream: z-score detection → Kafka alerts")
      println("   Press Ctrl+C to stop")
      println("═══════════════════════════════════════════════════")

      spark.streams.awaitAnyTermination()

    } catch {
      case e: Exception =>
        println(s"❌ Fatal error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      println("\n✓ Spark session stopped")
    }
  }
}

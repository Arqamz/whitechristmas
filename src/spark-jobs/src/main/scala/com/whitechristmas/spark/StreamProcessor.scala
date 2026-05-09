package com.whitechristmas.spark

import org.apache.spark.sql._
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.{
  avg,
  col,
  concat,
  count,
  current_timestamp,
  lit,
  max,
  round,
  stddev,
  struct,
  sum,
  to_json,
  udf,
  unix_millis,
  when,
  window
}
import org.apache.spark.sql.types._
import org.bson.Document
import com.mongodb.client.MongoClients
import scala.jdk.CollectionConverters._
import scala.io.Source

/** Spark Structured Streaming job for WhiteChristmas pipeline.
  *
  * Flow: Kafka raw-events-* (Avro + Confluent wire format) → strip 5-byte
  * header → from_avro → enrich → foreachBatch: PostgreSQL (all events) +
  * MongoDB alerts + Kafka per-dataset alerts topics
  *
  * Called standalone via main(), or as part of the combined Pipeline entry
  * point via setup() which registers all streams without blocking.
  */
object StreamProcessor {

  private def getConfig(key: String, default: String = ""): String =
    sys.props.getOrElse(key, sys.env.getOrElse(key, default))

  private def parsePostgresUrl(connStr: String): (String, String, String) = {
    val withoutScheme =
      connStr.stripPrefix("postgresql://").stripPrefix("postgres://")
    val atIdx = withoutScheme.lastIndexOf('@')
    val userPass = withoutScheme.substring(0, atIdx)
    val hostPortDb = withoutScheme.substring(atIdx + 1)
    val colonIdx = userPass.indexOf(':')
    val user = userPass.substring(0, colonIdx)
    val pass = userPass.substring(colonIdx + 1)
    (s"jdbc:postgresql://$hostPortDb", user, pass)
  }

  /** Register all StreamProcessor streaming queries against the given session.
    * Does NOT call awaitAnyTermination — callers are responsible for that. Safe
    * to call when a SparkSession already exists (getOrCreate is a no-op).
    */
  def setup(spark: SparkSession): Unit = {

    val kafkaBrokers = getConfig("KAFKA_BROKERS", "localhost:9092")
    val inputTopicPattern = getConfig("STREAM_INPUT_PATTERN", "raw-events-.*")
    val anomalyTopic = getConfig("ANOMALY_TOPIC", "anomaly-alerts")
    val checkpointDir = "/tmp/spark-checkpoint/stream-processor"
    val schemaPath =
      getConfig("AVRO_SCHEMA_PATH", "../schemas/crime-event.avsc")

    val pgConnString = getConfig("POSTGRES_CONN_STRING")
    val mongoConnString = getConfig("MONGODB_CONN_STRING")
    val pgEnabled = pgConnString.nonEmpty
    val mongoEnabled = mongoConnString.nonEmpty

    println("╔════════════════════════════════════════════════════╗")
    println("║  🎯 StreamProcessor — event enrichment + routing  ║")
    println("╚════════════════════════════════════════════════════╝")
    println(s"📖 Input Pattern:   $inputTopicPattern")
    println(s"🚨 Anomaly Topic:   $anomalyTopic")
    println(s"📋 Avro Schema:     $schemaPath")
    println(s"🐘 PostgreSQL:     ${if (pgEnabled) "enabled" else "disabled"}")
    println(
      s"🍃 MongoDB:        ${if (mongoEnabled) "enabled" else "disabled"}"
    )
    println()

    val (pgJdbcUrl, pgUser, pgPass) =
      if (pgEnabled) parsePostgresUrl(pgConnString) else ("", "", "")

    val avroSchemaStr = Source.fromFile(schemaPath).mkString
    println("✓ Avro schema loaded")

    val stripConfluentHeader = udf((bytes: Array[Byte]) =>
      if (bytes != null && bytes.length > 5) bytes.slice(5, bytes.length)
      else bytes
    )

    println("📥 Connecting to Kafka...")

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribePattern", inputTopicPattern)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

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

    val enriched = parsed
      .withColumn("processed_at", current_timestamp())
      .withColumn(
        "received_lag_ms",
        (col("processed_at").cast("long") - col("kafka_timestamp")
          .cast("long")) * 1000
      )
      .withColumn("is_alert", col("severity") >= 3)
      .withColumn(
        "event_label",
        when(
          col("crime_type").isNotNull && col("crime_type") =!= "",
          col("crime_type")
        )
          .otherwise(col("injury_type"))
      )

    // ── Main query: all events → PG; severity>=3 → MongoDB + Kafka alerts-{dataset} ──
    println(
      "🚀 Starting main foreachBatch stream (PostgreSQL + MongoDB + Kafka)..."
    )

    val mainQuery = enriched.writeStream
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          batchDF.cache()
          val totalCount = batchDF.count()
          println(s"\n[StreamProcessor Batch $batchId] $totalCount events")

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
              println(s"  ✓ PostgreSQL events: $totalCount rows")
            } catch {
              case e: Exception =>
                println(s"  ✗ PostgreSQL write failed: ${e.getMessage}")
            }
          }

          val alertsBatch = batchDF.filter(col("severity") >= 3).cache()

          if (!alertsBatch.isEmpty) {
            val alertCount = alertsBatch.count()

            if (mongoEnabled) {
              try {
                val docs = alertsBatch.toJSON.collect().map(Document.parse)
                val client = MongoClients.create(mongoConnString)
                try {
                  client
                    .getDatabase("whitechristmas")
                    .getCollection("alerts")
                    .insertMany(docs.toList.asJava)
                  println(s"  ✓ MongoDB alerts: $alertCount docs")
                } finally { client.close() }
              } catch {
                case e: Exception =>
                  println(s"  ✗ MongoDB write failed: ${e.getMessage}")
              }
            }

            // Per-dataset alert routing via topic column:
            //   source_dataset=crimes        → alerts-crimes
            //   source_dataset=violence      → alerts-violence
            //   source_dataset=arrests       → alerts-arrests
            //   source_dataset=sex-offenders → alerts-sex-offenders
            try {
              alertsBatch
                .select(
                  concat(lit("alerts-"), col("source_dataset")).as("topic"),
                  col("event_id").as("key"),
                  to_json(
                    struct(
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
                      unix_millis(col("processed_at")).as(
                        "processed_timestamp_api"
                      )
                    )
                  ).as("value")
                )
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBrokers)
                .save()
              println(
                s"  ✓ Kafka alerts-{{dataset}}: $alertCount events routed"
              )
            } catch {
              case e: Exception =>
                println(s"  ✗ Kafka alert write failed: ${e.getMessage}")
            }
          }

          alertsBatch.unpersist()
          batchDF.unpersist()
        }
        ()
      }
      .option("checkpointLocation", s"$checkpointDir/main")
      .start()

    // ── Metrics query: 10-second windowed severity stats per district → console ──
    println("📊 Starting district metrics stream...")

    val metricsQuery = enriched
      .groupBy(window(col("processed_at"), "10 seconds"), col("district"))
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

    // ── Anomaly detection: rolling z-score per district (2-minute window) ────
    println("🔬 Starting z-score anomaly detection stream...")

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

    val anomalyQuery = windowedStats.writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          val anomalies = batchDF
            .withColumn(
              "z_score",
              when(
                col("stddev_severity").isNotNull && col("stddev_severity") > 0,
                (col("max_severity") - col("mean_severity")) / col(
                  "stddev_severity"
                )
              ).otherwise(lit(0.0))
            )
            .filter(
              col("z_score") > 2.0 ||
                (col("stddev_severity").isNull && col("max_severity") >= 4)
            )

          if (!anomalies.isEmpty) {
            println(s"\n[Z-Score Batch $batchId] 🚨 Anomalies detected:")
            anomalies
              .select(
                col("district"),
                col("window.start").as("window_start"),
                col("window_count"),
                round(col("mean_severity"), 2).as("mean_sev"),
                round(col("stddev_severity"), 2).as("stddev_sev"),
                col("max_severity"),
                round(col("z_score"), 2).as("z_score")
              )
              .show(10, truncate = false)

            try {
              anomalies
                .select(
                  col("district").as("key"),
                  to_json(
                    struct(
                      lit("z_score").as("alert_type"),
                      col("district"),
                      col("window.start").as("window_start"),
                      col("window_count"),
                      round(col("mean_severity"), 2).as("mean_severity"),
                      round(col("stddev_severity"), 2).as("stddev_severity"),
                      col("max_severity"),
                      round(col("z_score"), 2).as("z_score"),
                      lit(System.currentTimeMillis()).as("detected_at")
                    )
                  ).as("value")
                )
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBrokers)
                .option("topic", anomalyTopic)
                .save()
              println(s"  ✓ Z-score anomalies → $anomalyTopic")
            } catch {
              case e: Exception =>
                println(s"  ✗ Anomaly Kafka write failed: ${e.getMessage}")
            }
          }
        }
      }
      .option("checkpointLocation", s"$checkpointDir/anomaly")
      .start()

    println()
    println("  StreamProcessor streams active:")
    println(
      "    main    → PostgreSQL events + MongoDB alerts + Kafka alerts-{dataset}"
    )
    println("    metrics → windowed district stats (console)")
    println("    anomaly → z-score detection → anomaly-alerts")
    println()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("WhiteChristmas-StreamProcessor")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    println(
      s"📊 Kafka Brokers: ${sys.props.getOrElse("KAFKA_BROKERS", sys.env.getOrElse("KAFKA_BROKERS", "localhost:9092"))}"
    )

    try {
      setup(spark)
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

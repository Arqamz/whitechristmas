package com.whitechristmas.spark

import org.apache.spark.sql._
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._
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
          col("event.victim_id"),
          col("event.incident_date"),
          col("event.incident_time"),
          col("event.location"),
          col("event.district"),
          col("event.injury_type"),
          col("event.severity"),
          col("event.processed_timestamp")
        )

      // Enrich with server-side timestamps and a high-severity flag
      val enriched = parsed
        .withColumn("processed_at", current_timestamp())
        .withColumn(
          "received_lag_ms",
          (col("processed_at").cast("long") - col("kafka_timestamp").cast("long")) * 1000
        )
        .withColumn("is_alert", col("severity") >= 3)

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
                      col("victim_id"),
                      col("incident_date"),
                      col("incident_time"),
                      col("location"),
                      col("district"),
                      col("injury_type"),
                      col("severity"),
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

      println()
      println("═══════════════════════════════════════════════════")
      println("✅ All streams started — awaiting events...")
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

package com.whitechristmas.spark

import org.apache.spark.sql._
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.io.Source

/**
 * Spark Structured Streaming job for WhiteChristmas pipeline.
 *
 * Flow: Kafka raw-events (Avro + Confluent wire format)
 *         → strip 5-byte header → from_avro → enrich → alerts topic
 *
 * Confluent wire format: [0x00][schema_id: 4 bytes BE][avro binary payload]
 */
object StreamProcessor {

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

    val kafkaBrokers  = sys.env.getOrElse("KAFKA_BROKERS", "localhost:9092")
    val inputTopic    = "raw-events"
    val alertsTopic   = "alerts"
    val checkpointDir = "/tmp/spark-checkpoint"

    // Schema path relative to sbt working directory (src/spark-jobs/)
    val schemaPath = sys.env.getOrElse(
      "AVRO_SCHEMA_PATH",
      "../schemas/crime-event.avsc"
    )

    println(s"📊 Kafka Brokers: $kafkaBrokers")
    println(s"📖 Input Topic:   $inputTopic")
    println(s"📤 Output Topic:  $alertsTopic")
    println(s"📋 Avro Schema:   $schemaPath")
    println()

    // Load Avro schema string for from_avro()
    val avroSchemaStr = Source.fromFile(schemaPath).mkString
    println("✓ Avro schema loaded")

    // UDF: strip the 5-byte Confluent wire format header [magic(1) + schemaId(4)]
    val stripConfluentHeader = udf((bytes: Array[Byte]) =>
      if (bytes != null && bytes.length > 5) bytes.slice(5, bytes.length)
      else bytes
    )

    spark.udf.register("stripConfluentHeader", stripConfluentHeader)

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

      // Strip header, deserialize Avro, flatten to top-level columns
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

      // Enrich: add server-side processing timestamp and lag
      val enriched = parsed
        .withColumn("processed_at", current_timestamp())
        .withColumn("received_lag_ms",
          (col("processed_at").cast("long") - col("kafka_timestamp").cast("long")) * 1000)
        .withColumn("is_alert", col("severity") >= 1)

      println("\n📋 Enriched schema:")
      enriched.printSchema()

      // Stream 1: console output for debugging
      println("📺 Starting console output stream...")
      val consoleQuery = enriched
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "5")
        .start()

      // Stream 2: forward alerts (severity >= 3) to alerts topic as JSON
      println("🚨 Starting alerts → Kafka stream...")
      val alertsQuery = enriched
        .filter(col("is_alert"))
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
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBrokers)
        .option("topic", alertsTopic)
        .option("checkpointLocation", s"$checkpointDir/alerts")
        .outputMode("append")
        .start()

      // Stream 3: 10-second windowed severity metrics per district
      println("📊 Starting district metrics stream...")
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

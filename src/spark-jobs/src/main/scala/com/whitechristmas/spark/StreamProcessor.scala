package com.whitechristmas.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Minimal Spark Structured Streaming job for processing crime events
 * 
 * Reads from Kafka raw-events topic, performs basic validation and enrichment,
 * and writes results to PostgreSQL and/or Kafka alerts topic
 */
object StreamProcessor {

  def main(args: Array[String]): Unit = {
    // Create Spark session with streaming support
    val spark = SparkSession
      .builder()
      .appName("WhiteChristmas-StreamProcessor")
      .master("local[*]") // Use all available cores
      .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
      .config("spark.sql.streaming.schemaInference", "true")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    // Set log level
    spark.sparkContext.setLogLevel("INFO")

    println("╔════════════════════════════════════════════════════╗")
    println("║  🎯 WhiteChristmas Spark Stream Processor         ║")
    println("╚════════════════════════════════════════════════════╝")
    println()

    // Configuration
    val kafkaBrokers = sys.env.getOrElse("KAFKA_BROKERS", "localhost:9092")
    val inputTopic = "raw-events"
    val alertsTopic = "alerts"
    val checkpointDir = "/tmp/spark-checkpoint"

    println(s"📊 Kafka Brokers: $kafkaBrokers")
    println(s"📖 Input Topic:   $inputTopic")
    println(s"📤 Output Topic:  $alertsTopic")
    println()

    try {
      // Read from Kafka with JSON format
      println("📥 Reading from Kafka...")
      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBrokers)
        .option("subscribe", inputTopic)
        .option("startingOffsets", "latest") // Only new messages for demo
        .option("failOnDataLoss", "false")
        .load()

      // Parse JSON payload
      val schema = StructType(Seq(
        StructField("event_id", StringType),
        StructField("victim_id", StringType),
        StructField("incident_date", StringType),
        StructField("incident_time", StringType),
        StructField("location", StringType),
        StructField("district", StringType),
        StructField("injury_type", StringType),
        StructField("severity", IntegerType),
        StructField("processed_timestamp", LongType)
      ))

      val parsed = df
        .select(
          col("key").cast(StringType).as("event_id"),
          col("timestamp").as("kafka_timestamp"),
          from_json(col("value").cast(StringType), schema).as("event")
        )
        .select(
          col("event_id"),
          col("kafka_timestamp"),
          col("event.*")
        )

      // Add processing timestamp and window information
      val enriched = parsed
        .withColumn("processed_at", current_timestamp())
        .withColumn("received_lag_ms", 
          col("processed_at").cast("long") * 1000 - col("kafka_timestamp"))
        .withColumn("is_alert", col("severity") >= 3)

      // Log schema
      println("\n📋 Event Schema:")
      enriched.printSchema()

      // Write to console (for demo)
      println("\n📺 Starting streaming output to console...")
      val consoleQuery = enriched
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "10")
        .start()

      // Write high-severity alerts to alerts topic
      println("🚨 Starting alerts stream to Kafka...")
      val alertsQuery = enriched
        .filter(col("is_alert"))
        .select(
          col("event_id").as("key"),
          to_json(struct(
            col("event_id"),
            col("incident_date"),
            col("incident_time"),
            col("location"),
            col("district"),
            col("severity"),
            col("processed_at"),
            col("received_lag_ms")
          )).as("value")
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBrokers)
        .option("topic", alertsTopic)
        .option("checkpointLocation", s"$checkpointDir/alerts")
        .outputMode("append")
        .start()

      // Alternative: Write aggregated metrics
      println("📊 Starting metrics aggregation...")
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
      println("✅ Spark Streaming Started!")
      println("═══════════════════════════════════════════════════")
      println()
      println("📊 Streams Running:")
      println("  1. Console output (raw events)")
      println("  2. Alerts to Kafka topic")
      println("  3. Metrics aggregation")
      println()
      println("Press Ctrl+C to stop...")
      println()

      // Wait for all queries to terminate
      spark.streams.awaitAnyTermination()

    } catch {
      case e: Exception =>
        println(s"❌ Error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      println("\n✓ Spark session closed")
    }
  }
}

package com.whitechristmas.spark

import org.apache.spark.sql.SparkSession

/** WhiteChristmas Production Pipeline — combined entry point.
  *
  * Starts both streaming jobs in a single SparkSession:
  *
  * StreamProcessor — per-event enrichment and routing raw-events-.* →
  * PostgreSQL events (all) → MongoDB alerts (severity >= 3) → Kafka
  * alerts-{dataset} (per-row routing, consumed by Go API) → Kafka
  * anomaly-alerts (z-score anomalies) → console (windowed district metrics)
  *
  * BoltPipeline — sliding-window volume anomaly detection raw-events-.* →
  * PostgreSQL alerts (windowed surges) → MongoDB alert_logs (windowed
  * aggregates) → Kafka anomaly-alerts (volume threshold breaches)
  *
  * The two jobs are complementary, not redundant:
  *   - StreamProcessor catches individual serious events instantly (per
  *     severity)
  *   - BoltPipeline catches sustained volume surges over time (per
  *     district+dataset)
  *   - Both consume raw-events-* independently (distinct Kafka consumer groups)
  *   - Only StreamProcessor writes to alerts-{dataset} (Go API CrimeEvent
  *     schema)
  *   - Both write anomaly signals to anomaly-alerts (different alert_type
  *     fields)
  *
  * This is the default `sbt run` target.
  */
object Pipeline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("WhiteChristmas-Pipeline")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("╔══════════════════════════════════════════════════════════════╗")
    println("║  🚀 WhiteChristmas Production Pipeline                      ║")
    println("║     StreamProcessor + BoltPipeline — single SparkSession    ║")
    println("╚══════════════════════════════════════════════════════════════╝")
    println()

    try {
      // Register all StreamProcessor streams (main, metrics, z-score anomaly)
      StreamProcessor.setup(spark)

      // Register all BoltPipeline streams (parse, window, volume anomaly alert)
      BoltPipeline.setup(spark)

      println("═══════════════════════════════════════════════════════════════")
      println("✅ All pipeline streams active — awaiting events...")
      println()
      println("  Stream               Output")
      println("  ─────────────────── ──────────────────────────────────────")
      println("  SP main             PostgreSQL events + MongoDB alerts")
      println("  SP main             Kafka alerts-{dataset} (Go API feed)")
      println("  SP metrics          Console (windowed district stats)")
      println("  SP z-score          Kafka anomaly-alerts")
      println("  BP window           Sliding count per dataset+district")
      println("  BP alert            PostgreSQL alerts + MongoDB alert_logs")
      println("  BP alert            Kafka anomaly-alerts (volume surges)")
      println()
      println("  Press Ctrl+C to stop all streams")
      println("═══════════════════════════════════════════════════════════════")

      spark.streams.awaitAnyTermination()

    } catch {
      case e: Exception =>
        println(s"❌ Fatal error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      println("\n✓ Pipeline stopped")
    }
  }
}

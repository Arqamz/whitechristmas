name := "whitechristmas-spark"
version := "0.1.0"
scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "4.0.1",
  "org.apache.spark" %% "spark-sql" % "4.0.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "4.0.1",
  "org.apache.spark" %% "spark-streaming" % "4.0.1",
  "org.apache.spark" %% "spark-avro" % "4.0.1",
  "org.apache.spark" %% "spark-mllib" % "4.0.1",
  "io.confluent" % "kafka-avro-serializer" % "7.5.0",
  "org.apache.avro" % "avro" % "1.11.3",
  "org.mongodb" % "mongodb-driver-sync" % "5.2.1",
  "org.postgresql" % "postgresql" % "42.7.4",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)

resolvers ++= Seq(
  "Confluent" at "https://packages.confluent.io/maven/"
)

// Auto-load ../../.env into JVM system properties so sys.env picks them up at runtime
run / javaOptions ++= {
  val envFile = new java.io.File("../../.env")
  if (envFile.exists()) {
    scala.io.Source
      .fromFile(envFile)
      .getLines()
      .map(_.trim)
      .filterNot(l => l.isEmpty || l.startsWith("#"))
      .filter(_.contains("="))
      .map { line =>
        val idx = line.indexOf('=')
        s"-D${line.substring(0, idx)}=${line.substring(idx + 1)}"
      }
      .toSeq
  } else Seq.empty
}

// Required for javaOptions to take effect with sbt run
fork := true

// Default entry point: combined production pipeline (StreamProcessor + BoltPipeline).
// For individual jobs:
//   sbt "runMain com.whitechristmas.spark.StreamProcessor"
//   sbt "runMain com.whitechristmas.spark.BoltPipeline"
//   sbt "runMain com.whitechristmas.spark.BatchAnalytics"
Compile / mainClass := Some("com.whitechristmas.spark.Pipeline")

// ── Assembly (fat JAR for Docker) ────────────────────────────────────────────
assembly / assemblyJarName := "whitechristmas-spark-assembly.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF")  => MergeStrategy.discard
  case PathList("META-INF", "INDEX.LIST")   => MergeStrategy.discard
  case PathList("META-INF", "DEPENDENCIES") => MergeStrategy.discard
  case PathList("META-INF", "LICENSE")      => MergeStrategy.discard
  case PathList("META-INF", "LICENSE.txt")  => MergeStrategy.discard
  case PathList("META-INF", "NOTICE")       => MergeStrategy.discard
  case PathList("META-INF", "NOTICE.txt")   => MergeStrategy.discard
  case PathList("META-INF", "io.netty.versions.properties") =>
    MergeStrategy.discard
  case PathList("META-INF", "native-image", _*) => MergeStrategy.discard
  case PathList("META-INF", "services", _*)     => MergeStrategy.concat
  case PathList("META-INF", _*)                 => MergeStrategy.discard
  case PathList("reference.conf")               => MergeStrategy.concat
  case PathList("application.conf")             => MergeStrategy.concat
  case PathList("log4j2.properties")            => MergeStrategy.first
  case PathList("module-info.class")            => MergeStrategy.discard
  case _                                        => MergeStrategy.first
}

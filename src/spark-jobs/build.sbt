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

name := "whitechristmas-spark"
version := "0.1.0"
scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "4.0.1",
  "org.apache.spark" %% "spark-sql" % "4.0.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "4.0.1",
  "org.apache.spark" %% "spark-streaming" % "4.0.1",
  "org.apache.spark" %% "spark-avro" % "4.0.1",
  "io.confluent" % "kafka-avro-serializer" % "7.5.0",
  "org.apache.avro" % "avro" % "1.11.3",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
)

resolvers ++= Seq(
  "Confluent" at "https://packages.confluent.io/maven/",
)

# ── Stage 1: Build fat JAR ────────────────────────────────────────────────────
FROM eclipse-temurin:17-jdk-jammy AS builder

ARG SBT_VERSION=1.10.7
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/* && \
    curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
      | tar xz -C /usr/local
ENV PATH="/usr/local/sbt/bin:${PATH}"

WORKDIR /build

# Cache dependency resolution — only re-downloads when build.sbt / plugins change
COPY src/spark-jobs/project ./project
COPY src/spark-jobs/build.sbt ./
RUN sbt update

# Compile and assemble fat JAR
COPY src/spark-jobs/src ./src
RUN sbt assembly

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
# eclipse-temurin:17-jre gives us a lean JVM without build tooling
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

COPY --from=builder /build/target/scala-2.13/whitechristmas-spark-assembly.jar /app/app.jar

# Avro schema read at runtime from this path (matches AVRO_SCHEMA_PATH env var)
COPY src/schemas /schemas

ENV AVRO_SCHEMA_PATH=/schemas/crime-event.avsc

# StreamProcessor sets master("local[*]") internally; heap tuned for streaming
ENTRYPOINT ["java", \
  "-Xmx4g", \
  "-Xms1g", \
  "-XX:+UseG1GC", \
  "-Dlog4j.configurationFile=/app/log4j2.properties", \
  "-cp", "/app/app.jar", \
  "com.whitechristmas.spark.StreamProcessor"]

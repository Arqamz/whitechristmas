package com.whitechristmas.spark

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.File
import scala.jdk.CollectionConverters._

/** WhiteChristmas Batch Analytics — sections 7.1 through 7.6.
  *
  * Reads the five Chicago public-safety CSV files directly and writes
  * aggregated results to PostgreSQL.
  *
  * Tables written: crime_trends (7.1) arrest_rate_analysis (7.2)
  * top_arrest_rate_crime_types (7.2) violence_analysis (7.3)
  * top_violence_community (7.3) sex_offender_proximity (7.4)
  * district_offender_density (7.4) hotspots (7.5) correlations (7.6)
  *
  * Run: sbt "runMain com.whitechristmas.spark.BatchAnalytics" or: spark-submit
  * --class com.whitechristmas.spark.BatchAnalytics \
  * target/scala-2.13/whitechristmas-spark-assembly-0.1.0.jar
  *
  * Environment variables: POSTGRES_CONN_STRING
  * postgresql://user:pass@host:port/db (required) DATA_DIR path to Chicago CSV
  * files (default: ../../data) KMEANS_K number of K-Means clusters (default:
  * 10) MIN_CRIMES_FOR_RATE min crimes for arrest-rate stat (default: 100)
  */
object BatchAnalytics {

  private def getConfig(key: String, default: String = ""): String =
    sys.props.getOrElse(key, sys.env.getOrElse(key, default))

  private def parsePostgresUrl(connStr: String): (String, String, String) = {
    val s = connStr.stripPrefix("postgresql://").stripPrefix("postgres://")
    val atIdx = s.lastIndexOf('@')
    val up = s.substring(0, atIdx)
    val hostDb = s.substring(atIdx + 1)
    val ci = up.indexOf(':')
    (s"jdbc:postgresql://$hostDb", up.substring(0, ci), up.substring(ci + 1))
  }

  private def pgWrite(
      df: DataFrame,
      table: String,
      url: String,
      user: String,
      pass: String,
      mode: String = "overwrite"
  ): Unit = {
    df.write
      .mode(mode)
      .format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("driver", "org.postgresql.Driver")
      .option("user", user)
      .option("password", pass)
      .option("stringtype", "unspecified")
      .save()
    println(s"  ✓  Written → PostgreSQL table '$table'")
  }

  /** Find the first CSV whose name contains `keyword` (case-insensitive). */
  private def findCsv(dir: String, keyword: String): String =
    new File(dir)
      .listFiles()
      .filter(f =>
        f.getName.toLowerCase.contains(keyword.toLowerCase) && f.getName
          .endsWith(".csv")
      )
      .headOption
      .map(_.getAbsolutePath)
      .getOrElse(
        throw new RuntimeException(s"No CSV matching '$keyword' in $dir")
      )

  def main(args: Array[String]): Unit = {

    val pgConnStr = getConfig("POSTGRES_CONN_STRING")
    if (pgConnStr.isEmpty) {
      println("ERROR: POSTGRES_CONN_STRING not set"); sys.exit(1)
    }

    val dataDir = getConfig("DATA_DIR", "../../data")
    val k = getConfig("KMEANS_K", "10").toInt
    val minCrimesForRate = getConfig("MIN_CRIMES_FOR_RATE", "100").toInt

    val (pgUrl, pgUser, pgPass) = parsePostgresUrl(pgConnStr)

    val spark = SparkSession
      .builder()
      .appName("WhiteChristmas-BatchAnalytics")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.driver.memory", "4g")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("╔═══════════════════════════════════════════════════════╗")
    println("║  WhiteChristmas Batch Analytics  (7.1 – 7.6)        ║")
    println("╚═══════════════════════════════════════════════════════╝")
    println(s"  Data dir : $dataDir")
    println(s"  K-Means k: $k")
    println()

    def readCsv(keyword: String): DataFrame = {
      val path = findCsv(dataDir, keyword)
      println(s"  Loading $path")
      spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", "\"")
        .csv(path)
    }

    val crimesDF = readCsv("rimes") // Crimes_-_2001_to_Present_...csv
    val arrestsDF = readCsv("rrests") // Arrests_...csv
    val violenceDF = readCsv("iolence") // Violence_Reduction_...csv
    val sexOffDF = readCsv("ex_Off") // Sex_Offenders_...csv
    val stationsDF = readCsv("tations") // Police_Stations_...csv

    // ───────────────────────────────────────────────────────────────────────
    // 7.1  Crime Trend Analysis
    // Aggregated crime counts by year, month, day-of-week, hour → crime_trends
    // ───────────────────────────────────────────────────────────────────────
    println("\n[7.1] Crime Trend Analysis")

    val crimesTs = crimesDF.withColumn(
      "_ts",
      to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")
    )

    val crimeTrends = crimesTs
      .withColumn("year", year(col("_ts")))
      .withColumn("month", month(col("_ts")))
      .withColumn("day_of_week", dayofweek(col("_ts")))
      .withColumn("hour", hour(col("_ts")))
      .groupBy("year", "month", "day_of_week", "hour")
      .agg(count("*").as("crime_count"))
      .orderBy("year", "month", "day_of_week", "hour")

    pgWrite(crimeTrends, "crime_trends", pgUrl, pgUser, pgPass)
    println(s"  Rows: ${crimeTrends.count()}")

    // ───────────────────────────────────────────────────────────────────────
    // 7.2  Arrest Rate Analysis
    // Join Crimes ⟕ Arrests on Case Number.
    // Arrest rate by crime type, district, race.
    // Top 10 crime types by arrest rate.
    // ───────────────────────────────────────────────────────────────────────
    println("\n[7.2] Arrest Rate Analysis")

    // Alias columns with spaces immediately to avoid repeated escaping
    val crimesForJoin = crimesDF.select(
      col("`Case Number`").as("case_number"),
      col("`Primary Type`").as("primary_type"),
      col("District").as("district"),
      col("Arrest").as("arrest_flag")
    )

    val arrestsForJoin = arrestsDF
      .select(
        col("`CASE NUMBER`").as("case_number"),
        col("RACE").as("race")
      )
      .dropDuplicates("case_number")

    val joined = crimesForJoin.join(arrestsForJoin, Seq("case_number"), "left")

    val isArrested = when(
      lower(trim(col("arrest_flag"))).isin("true", "yes", "1"),
      1
    ).otherwise(0)

    val arrestRateDF = joined
      .groupBy("primary_type", "district", "race")
      .agg(
        count("*").as("total_crimes"),
        sum(isArrested).as("total_arrests")
      )
      .withColumn(
        "arrest_rate",
        round(col("total_arrests") / col("total_crimes"), 4)
      )

    pgWrite(arrestRateDF, "arrest_rate_analysis", pgUrl, pgUser, pgPass)

    val top10DF = joined
      .groupBy("primary_type")
      .agg(
        count("*").as("total_crimes"),
        sum(isArrested).as("total_arrests")
      )
      .filter(col("total_crimes") >= minCrimesForRate)
      .withColumn(
        "arrest_rate",
        round(col("total_arrests") / col("total_crimes"), 4)
      )
      .orderBy(col("arrest_rate").desc)
      .limit(10)

    pgWrite(top10DF, "top_arrest_rate_crime_types", pgUrl, pgUser, pgPass)
    println("  Top 10 crime types by arrest rate:")
    top10DF.show(truncate = false)

    // ───────────────────────────────────────────────────────────────────────
    // 7.3  Violence and Gunshot Analysis
    // (a) Homicides vs non-fatal shootings by month and district
    // (b) Proportion of incidents with confirmed gunshot injury (GUNSHOT_INJURY_I = YES)
    // (c) Top community areas by violence incidence
    // ───────────────────────────────────────────────────────────────────────
    println("\n[7.3] Violence and Gunshot Analysis")

    // Violence CSV has MONTH as a separate column; GUNSHOT_INJURY_I = "YES"/"NO"/""
    val violenceClean = violenceDF
      .withColumn("_ts", to_timestamp(col("DATE"), "MM/dd/yyyy hh:mm:ss a"))
      .withColumn(
        "month",
        coalesce(col("MONTH").cast("int"), month(col("_ts")))
      )
      .withColumn(
        "is_homicide",
        when(
          lower(col("VICTIMIZATION_PRIMARY")).contains("homicide") ||
            lower(col("INCIDENT_PRIMARY")).contains("homicide"),
          1
        ).otherwise(0)
      )
      .withColumn(
        "is_gunshot",
        when(
          upper(trim(col("GUNSHOT_INJURY_I"))) === "YES",
          1
        ).otherwise(0)
      )
      .withColumn(
        "is_non_fatal_shooting",
        when(
          col("is_gunshot") === 1 && col("is_homicide") === 0,
          1
        ).otherwise(0)
      )

    // (a) Monthly / district breakdown
    val violenceMonthly = violenceClean
      .groupBy(col("month"), col("DISTRICT").as("district"))
      .agg(
        sum("is_homicide").as("homicides"),
        sum("is_non_fatal_shooting").as("non_fatal_shootings"),
        sum("is_gunshot").as("gunshot_incidents"),
        count("*").as("total_incidents")
      )
      .orderBy("month", "district")

    // (b) Gunshot proportion scalar — attach as a constant column
    val totalsRow = violenceClean
      .agg(
        count("*").as("total"),
        sum("is_gunshot").as("gunshot_total")
      )
      .collect()
      .head

    val gsTotal = totalsRow.getLong(totalsRow.fieldIndex("gunshot_total"))
    val total = totalsRow.getLong(totalsRow.fieldIndex("total"))
    val gsProportion =
      if (total > 0)
        BigDecimal(gsTotal.toDouble / total.toDouble)
          .setScale(4, BigDecimal.RoundingMode.HALF_UP)
          .toDouble
      else 0.0

    val violenceAnalysis = violenceMonthly.withColumn(
      "gunshot_proportion_overall",
      lit(gsProportion)
    )

    pgWrite(violenceAnalysis, "violence_analysis", pgUrl, pgUser, pgPass)
    println(
      f"  Gunshot proportion (all violence incidents): ${gsProportion * 100}%.2f%%"
    )

    // (c) Top community areas
    val topCommunity = violenceDF
      .filter(
        col("COMMUNITY_AREA").isNotNull && trim(col("COMMUNITY_AREA")) =!= ""
      )
      .groupBy(col("COMMUNITY_AREA").as("community_area"))
      .agg(count("*").as("incident_count"))
      .orderBy(col("incident_count").desc)
      .limit(20)

    pgWrite(topCommunity, "top_violence_community", pgUrl, pgUser, pgPass)
    println("  Top community areas by violence incidence:")
    topCommunity.show(truncate = false)

    // ───────────────────────────────────────────────────────────────────────
    // 7.4  Sex Offender Proximity Analysis
    // Join Sex Offenders with Police Stations on district.
    // Flag VICTIM MINOR = 'Y' records for priority reporting.
    //
    // Note: the Sex Offenders CSV has no DISTRICT column; we flag all
    // VICTIM MINOR = 'Y' records and attach the city-wide offender count
    // to each police station row for district-level density reporting.
    // ───────────────────────────────────────────────────────────────────────
    println("\n[7.4] Sex Offender Proximity Analysis")

    val sexOffFlagged = sexOffDF
      .select(
        concat_ws(" ", trim(col("FIRST")), trim(col("LAST")))
          .as("offender_name"),
        col("BLOCK").as("block"),
        col("RACE").as("race"),
        col("GENDER").as("gender"),
        col("`VICTIM MINOR`").as("victim_minor")
      )
      .withColumn(
        "is_victim_minor",
        when(upper(trim(col("victim_minor"))) === "Y", true).otherwise(false)
      )
      .withColumn(
        "priority_flag",
        when(col("is_victim_minor"), lit("PRIORITY")).otherwise(lit("STANDARD"))
      )

    pgWrite(sexOffFlagged, "sex_offender_proximity", pgUrl, pgUser, pgPass)

    val totalOffenders = sexOffFlagged.count()
    val minorOffenders = sexOffFlagged.filter(col("is_victim_minor")).count()

    // District density: police stations + city-wide offender counts
    val stationsRef = stationsDF.select(
      col("DISTRICT").as("district"),
      col("`DISTRICT NAME`").as("district_name"),
      col("ADDRESS").as("station_address"),
      col("LATITUDE").cast("double").as("station_lat"),
      col("LONGITUDE").cast("double").as("station_lon")
    )

    val districtDensity = stationsRef
      .withColumn("total_registered_offenders", lit(totalOffenders))
      .withColumn("minor_victim_offenders", lit(minorOffenders))

    pgWrite(districtDensity, "district_offender_density", pgUrl, pgUser, pgPass)

    println(s"  Total registered offenders : $totalOffenders")
    println(s"  Flagged VICTIM MINOR = 'Y' : $minorOffenders")

    // ───────────────────────────────────────────────────────────────────────
    // 7.5  Geospatial Hotspot Detection — K-Means (k configurable, default 10)
    // Apply K-Means to crime (LATITUDE, LONGITUDE) via Spark MLlib.
    // Store cluster centroids and labels in the hotspots table.
    // ───────────────────────────────────────────────────────────────────────
    println(s"\n[7.5] K-Means Hotspot Detection  (k=$k)")

    val crimesGeo = crimesDF
      .filter(
        col("Latitude").isNotNull && col("Latitude") =!= "" &&
          col("Longitude").isNotNull && col("Longitude") =!= ""
      )
      .select(
        col("`Case Number`").as("case_number"),
        col("`Primary Type`").as("primary_type"),
        col("District").as("district"),
        col("Latitude").cast("double").as("latitude"),
        col("Longitude").cast("double").as("longitude")
      )
      .filter(col("latitude").isNotNull && col("longitude").isNotNull)
      .cache()

    println(s"  Records with valid coordinates: ${crimesGeo.count()}")

    val assembler = new VectorAssembler()
      .setInputCols(Array("latitude", "longitude"))
      .setOutputCol("features")

    val geoVectors = assembler.transform(crimesGeo)

    val kmeansModel = new KMeans()
      .setK(k)
      .setSeed(42L)
      .setMaxIter(30)
      .setFeaturesCol("features")
      .setPredictionCol("cluster_id")
      .fit(geoVectors)

    val predictions = kmeansModel.transform(geoVectors)

    // Build centroid DataFrame
    val centroidRows =
      kmeansModel.clusterCenters.zipWithIndex.map { case (center, i) =>
        Row(i, center(0), center(1))
      }
    val centroidSchema = StructType(
      Array(
        StructField("cluster_id", IntegerType, nullable = false),
        StructField("centroid_lat", DoubleType, nullable = false),
        StructField("centroid_lon", DoubleType, nullable = false)
      )
    )
    val centroidDF = spark.createDataFrame(
      spark.sparkContext.parallelize(centroidRows.toSeq),
      centroidSchema
    )

    // Per-cluster stats
    val clusterStats = predictions
      .groupBy(col("cluster_id"))
      .agg(
        count("*").as("crime_count"),
        round(avg("latitude"), 6).as("avg_lat"),
        round(avg("longitude"), 6).as("avg_lon"),
        concat_ws(", ", collect_set("primary_type")).as("primary_types")
      )

    val hotspots = clusterStats
      .join(centroidDF, Seq("cluster_id"))
      .orderBy(col("crime_count").desc)

    pgWrite(hotspots, "hotspots", pgUrl, pgUser, pgPass)
    println(s"  ✓  $k clusters written to hotspots table")
    hotspots
      .select("cluster_id", "crime_count", "centroid_lat", "centroid_lon")
      .show()

    crimesGeo.unpersist()

    // ───────────────────────────────────────────────────────────────────────
    // 7.6  Cross-Dataset Correlation
    // Correlation 1: violence rate vs total arrest rate, grouped by district
    // Correlation 2: sex offender density vs crime rate, grouped by community area
    // Both written to the correlations table.
    // ───────────────────────────────────────────────────────────────────────
    println("\n[7.6] Cross-Dataset Correlation")

    val isArr = when(
      lower(trim(col("Arrest"))).isin("true", "yes", "1"),
      1
    ).otherwise(0)

    val crimeByDistrict = crimesDF
      .groupBy(col("District").as("district"))
      .agg(
        count("*").as("total_crimes"),
        sum(isArr).as("total_arrests")
      )
      .withColumn(
        "arrest_rate",
        round(col("total_arrests") / col("total_crimes"), 4)
      )

    val violenceByDistrict = violenceDF
      .filter(col("DISTRICT").isNotNull && trim(col("DISTRICT")) =!= "")
      .groupBy(col("DISTRICT").as("district"))
      .agg(count("*").as("violence_incidents"))

    val corr1 = crimeByDistrict
      .join(violenceByDistrict, Seq("district"), "left")
      .withColumn(
        "violence_rate",
        round(col("violence_incidents") / col("total_crimes"), 4)
      )
      .select(
        lit("violence_rate_vs_arrest_rate_by_district").as("correlation_type"),
        col("district").as("group_key"),
        lit(null).cast(StringType).as("community_area"),
        col("total_crimes"),
        col("total_arrests"),
        col("arrest_rate").as("metric_a"),
        col("violence_incidents"),
        col("violence_rate").as("metric_b"),
        lit(null).cast(LongType).as("sex_offender_count")
      )

    val crimesByCommunity = crimesDF
      .filter(
        col("`Community Area`").isNotNull &&
          trim(col("`Community Area`")) =!= ""
      )
      .groupBy(col("`Community Area`").as("community_area"))
      .agg(count("*").as("total_crimes"))

    val sexOffTotal = sexOffDF.count()

    val corr2 = crimesByCommunity.select(
      lit("sex_offender_density_vs_crime_rate_by_community").as(
        "correlation_type"
      ),
      lit(null).cast(StringType).as("group_key"),
      col("community_area"),
      col("total_crimes"),
      lit(null).cast(LongType).as("total_arrests"),
      col("total_crimes").cast(DoubleType).as("metric_a"),
      lit(null).cast(LongType).as("violence_incidents"),
      lit(null).cast(DoubleType).as("metric_b"),
      lit(sexOffTotal).cast(LongType).as("sex_offender_count")
    )

    val correlations = corr1.unionByName(corr2)
    pgWrite(correlations, "correlations", pgUrl, pgUser, pgPass)
    println(s"  ✓  correlations table: ${correlations.count()} rows")

    println("\n╔═══════════════════════════════════════════════════════╗")
    println("║  All analytics complete  (7.1 – 7.6)                ║")
    println("╚═══════════════════════════════════════════════════════╝")

    spark.stop()
  }
}

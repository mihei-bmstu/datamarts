import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object MainAppCalc {
  def main(args: Array[String]): Unit = {
      val hiveInitBD = "hdfs://ns-etl/warehouse/tablespace/external/hive/school_de.db"
      val spark = SparkSession.builder()
        .master("yarn")
        .appName("MainAppCalc_mvchernov")
        .config("spark.sql.warehouse.dir", hiveInitBD)
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.sql.session.timeZone", "UTC+3")
        .enableHiveSupport()
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

    val hiveTableResults = "school_de.results_mvchernov"

    val hiveTableTicketFlights = "school_de.bookings_ticket_flights"
    val hiveTableFlightsV = "school_de.bookings_flights_v"
    val hiveTableTickets = "school_de.bookings_tickets"
    val hiveTableAirports = "school_de.bookings_airports"
    val hiveTableAircrafts = "school_de.bookings_aircrafts"
    val hiveTableRoutes = "school_de.bookings_routes"
    val hiveTableFlights = "school_de.bookings_flights"

    //query 1
    val DFOne = spark.table(hiveTableTickets)
      .groupBy("book_ref")
      .agg(count("passenger_id").alias("cnt"))
      .agg(max("cnt").alias("max_cnt"))
      .withColumn("response", col("max_cnt").cast(StringType))
      .withColumn("id", lit(1))
      .select("id", "response")

    //query 2
    val passNmbr = spark.table(hiveTableTickets)
      .select("passenger_id")
      .count()

    val bookNmbr = spark.table(hiveTableTickets)
      .select("book_ref")
      .distinct()
      .count()

    val avgPassNmbr: Float = passNmbr.toFloat / bookNmbr.toFloat
    val DFTwo = spark.table(hiveTableTickets)
      .groupBy("book_ref")
      .agg(count("passenger_id").as("avg_pass"))
      .filter(col("avg_pass") > avgPassNmbr)
      .agg(count("book_ref").as("cnt"))
      .withColumn("response", col("cnt").cast(StringType))
      .withColumn("id", lit(2))
      .select("id","response")

    //query 3
    val maxBookNmbr = spark.table(hiveTableTickets)
      .groupBy("book_ref")
      .agg(count("passenger_id").alias("cnt"))
      .agg(max("cnt"))
      .first()
      .getLong(0)

    val maxBookRefList = spark.table(hiveTableTickets)
      .groupBy("book_ref")
      .agg(
        count("passenger_id").alias("cnt_pass"))
      .filter(col("cnt_pass") === maxBookNmbr )
      .select("book_ref")
      .collect()
      .map(f=>f.getString(0))

    val DFCount = spark.table(hiveTableTickets)
      .filter(col("book_ref").isin(maxBookRefList:_*))
      .orderBy("book_ref", "passenger_id")
      .groupBy("book_ref")
      .agg(concat_ws(
        ";",
        collect_list("passenger_id"))
      .alias("pass_list")
      )
      .groupBy("pass_list")
      .agg(count("pass_list").alias("cnt"))
      .filter(col("cnt") > 1)

    val columns = Seq("id", "response")
    val dataThree = Seq((3, DFCount.count().toString))
    val DFThree = spark.createDataFrame(dataThree).toDF(columns:_*)

    //query 4
    val bookRefList = spark.table(hiveTableTickets)
      .groupBy("book_ref")
      .agg(
        count("passenger_id").alias("cnt_pass"))
      .filter(col("cnt_pass") === 3 )
      .select("book_ref")
      .collect()
      .map(f=>f.getString(0))

    val DFFour = spark.table(hiveTableTickets)
      .filter(col("book_ref").isin(bookRefList:_*))
      .orderBy("book_ref", "passenger_id")
      .withColumn(
        "response",
        concat_ws("|",
          col("book_ref"),
          col("passenger_id"),
          col("passenger_name"),
          col("contact_data")))
      .withColumn("id", lit(4))
      .select("id","response")

    //query 5
    val DFFive = spark.table(hiveTableTickets)
      .join(spark.table(hiveTableTicketFlights), "ticket_no")
      .groupBy("book_ref")
      .agg(count("*").alias("cnt"))
      .agg(max("cnt").alias("cnt_max"))
      .withColumn("response", col("cnt_max").cast(StringType))
      .withColumn("id", lit(5))
      .select("id", "response")

    //query 6
    val DFSix = spark.table(hiveTableTickets)
      .join(spark.table(hiveTableTicketFlights), "ticket_no")
      .groupBy("book_ref", "passenger_id")
      .agg(count("*").alias("cnt"))
      .agg(max("cnt").alias("cnt_max"))
      .withColumn("response", col("cnt_max").cast(StringType))
      .withColumn("id", lit(6))
      .select("id", "response")

    //query 7
    val DFSeven = spark.table(hiveTableTickets)
      .join(spark.table(hiveTableTicketFlights), "ticket_no")
      .groupBy( "passenger_id")
      .agg(count("*").alias("cnt"))
      .agg(max("cnt").alias("cnt_max"))
      .withColumn("response", col("cnt_max").cast(StringType))
      .withColumn("id", lit(7))
      .select("id", "response")

    //query 8
    val DFEightRaw = spark.table(hiveTableFlights)
      .filter("status != 'Cancelled'")
      .join(spark.table(hiveTableTicketFlights), "flight_id")
      .join(spark.table(hiveTableTickets), "ticket_no")
      .withColumn("summa",
        sum(col("amount"))
          .over(Window.partitionBy("passenger_id"))
      )
      .orderBy(col("summa"))

    val minSum = DFEightRaw
      .select("summa")
      .first()
      .get(0)

    val DFEight = DFEightRaw
      .filter(col("summa") === minSum)
      .orderBy("passenger_id")
      .withColumn("id", lit(8))
      .withColumn("response",
        concat_ws("|",
          col("passenger_id"),
          col("passenger_name"),
          col("contact_data"),
          col("summa"))
      )
      .select("id","response")

    //query 9
    val DFNineRaw = spark.table(hiveTableFlightsV)
      .filter("status = 'Arrived'")
      .select("flight_id", "flight_no", "actual_duration")
      .withColumn("actual_duration_t",
        to_timestamp(col("actual_duration")))
      .withColumn("ad_min",
        minute(col("actual_duration_t")) + hour(col("actual_duration_t")) * 60)
      .join(spark.table(hiveTableTicketFlights), "flight_id")
      .join(spark.table(hiveTableTickets), "ticket_no")
      .withColumn("summa",
        sum(col("ad_min"))
          .over(Window.partitionBy("passenger_id"))
      )
      .orderBy(col("summa").desc)

    val maxDuration = DFNineRaw
      .select("summa")
      .first()
      .getLong(0)

    val hours = maxDuration / 60
    val minutes = maxDuration - 60 * hours
    val totalTime: String = hours.toString ++ ":" ++ minutes.toString ++ ":00"

    val DFNine = DFNineRaw
      .filter(col("summa") === maxDuration)
      .withColumn("summa_h", lit(totalTime))
      .orderBy("passenger_id")
      .withColumn("id", lit(9))
      .withColumn("response",
        concat_ws("|",
          col("passenger_id"),
          col("passenger_name"),
          col("contact_data"),
          col("summa_h"))
      )
      .select("id","response")
      .distinct()
      .orderBy("response")

        //query 10
    val DFTen = spark.table(hiveTableAirports)
      .groupBy("city")
      .agg(count("airport_name").alias("nmbr_airports"))
      .filter("nmbr_airports > 1")
      .withColumnRenamed("city","response")
      .withColumn("id", lit(10))
      .select("id","response")
      .orderBy("response")

    //query 11
    val minNumberCities = spark.table(hiveTableRoutes)
      .groupBy("departure_city")
      .agg(countDistinct("arrival_city").alias("cnt"))
      .orderBy("cnt")
      .select("cnt")
      .first()
      .getLong(0)

    val DFEleven = spark.table(hiveTableRoutes)
      .groupBy("departure_city")
      .agg(countDistinct("arrival_city").alias("cnt"))
      .filter(col("cnt") === minNumberCities)
      .orderBy("departure_city")
      .withColumnRenamed("departure_city", "response")
      .withColumn("id", lit(11))
      .select("id", "response")

    //query 12
    val air1 = spark.table(hiveTableAirports).select(col("city").as("depart"))
    val air2 = spark.table(hiveTableAirports).select(col("city").as("arrival"))
    val allCities = air1
      .crossJoin(air2)
      .where(col("depart") =!= col("arrival"))
      .orderBy("depart", "arrival")

    val directCities = spark.table(hiveTableRoutes)
      .select( "departure_city", "arrival_city")
      .distinct()

    val DFTwelve = allCities.except(directCities)
      .where("arrival > depart")
      .withColumn("response",
        concat_ws("|",
          col("depart"),
          col("arrival"))
      )
      .withColumn("id", lit(12))
      .select("id", "response")

    //query 13
    val citiesFromMoscow = spark.table(hiveTableRoutes)
      .filter(col("departure_city") === "Москва")
      .select("arrival_city")
      .distinct()
      .collect()
      .map(f=>f.getString(0))

    val DFThirteen = spark.table(hiveTableAirports)
      .filter(!col("city").isin(citiesFromMoscow:_*))
      .filter(col("city") =!= "Москва")
      .withColumn("id", lit(13))
      .withColumnRenamed("city", "response")
      .select("id", "response")
      .distinct()
      .orderBy("response")

    //query 14
    val topAircraftCode = spark.table(hiveTableFlights)
      .filter(col("status") === "Arrived")
      .groupBy("aircraft_code")
      .agg(count("flight_id").alias("cnt"))
      .orderBy(col("cnt").desc)
      .select("aircraft_code")
      .first()
      .getString(0)

    val DFFourteen = spark.table(hiveTableAircrafts)
      .filter(col("aircraft_code") === topAircraftCode)
      .withColumn("id", lit(14))
      .withColumnRenamed("model", "response")
      .select("id", "response")

    //query 15
    val DFFlightsFiltered = spark.table(hiveTableFlights)
      .filter(col("status") === "Arrived")
      .select("flight_id", "aircraft_code")

    val topPassAircraftCode = spark.table(hiveTableTicketFlights)
      .join(DFFlightsFiltered, "flight_id")
      .groupBy("aircraft_code")
      .agg(count("ticket_no").alias("cnt"))
      .orderBy(col("cnt").desc)
      .select("aircraft_code")
      .first()
      .getString(0)

    val DFFifteen = spark.table(hiveTableAircrafts)
      .filter(col("aircraft_code") === topPassAircraftCode)
      .withColumn("id", lit(15))
      .withColumnRenamed("model", "response")
      .select("id", "response")

    //query 16
    val DFSumMinutes = spark.table(hiveTableFlightsV)
      .filter(col("status") === "Arrived")
      .select("actual_duration", "scheduled_duration")
      .withColumn("actual_duration_t",
        to_timestamp(col("actual_duration")))
      .withColumn("ad_min",
        minute(col("actual_duration_t")) + hour(col("actual_duration_t")) * 60)
      .withColumn("scheduled_duration_t",
        to_timestamp(col("scheduled_duration")))
      .withColumn("sd_min",
        minute(col("scheduled_duration_t")) + hour(col("scheduled_duration_t")) * 60)
      .agg(sum("ad_min").as("sum_ad"),
        sum("sd_min").as("sum_sd"))
      .first()

    val columnsSixteen = Seq("id", "response")
    val dataSixteen = Seq((16, (DFSumMinutes.getLong(0) - DFSumMinutes.getLong(1)).toString))
    val DFSixteen = spark.createDataFrame(dataSixteen).toDF(columnsSixteen: _*)

    //query 17
    val DFSeventeen = spark.table(hiveTableFlightsV)
      .filter("departure_city = 'Санкт-Петербург'")
      .filter(col("status") === "Arrived"
        || col("status") === "Departed")
      .filter("DATE(actual_departure) = '2016-09-13'")
      .withColumn("id", lit(17))
      .withColumnRenamed("arrival_city", "response")
      .select("id", "response")
      .distinct()
      .orderBy("response")

    //query 18
    val DFEighteen = spark.table(hiveTableFlights)
      .filter(col("status") =!= "Cancelled")
      .join(spark.table(hiveTableTicketFlights), "flight_id")
      .groupBy("flight_id")
      .agg(sum("amount").alias("summa"))
      .orderBy(col("summa").desc)
      .withColumn("response", col("flight_id").cast(StringType))
      .withColumn("id", lit(18))
      .select("id", "response")
      .limit(1)

    //query 19
    val DFNineteen = spark.table(hiveTableFlights)
      .filter("actual_departure is not null")
      .filter(col("status") =!= "Cancelled")
      .withColumn("date", to_date(col("actual_departure")))
      .groupBy("date")
      .agg(count("flight_id").alias("cnt"))
      .orderBy("cnt")
      .withColumn("id", lit(19))
      .withColumn("response", col("date").cast(StringType))
      .select("id", "response")
      .limit(1)

    //query 20
    val avgNumberFlights = spark.table(hiveTableFlightsV)
      .withColumn("month", month(col("actual_departure")))
      .withColumn("year", year(col("actual_departure")))
      .filter("month == 9")
      .filter("year == 2016")
      .filter(col("departure_city") === "Москва")
      .filter("status in ('Arrived', 'Departed')")
      .count()

    val dataTwenty = Seq((20, (avgNumberFlights / 30).toString))
    val DFTwenty = spark.createDataFrame(dataTwenty).toDF(columns: _*)

    //query 21
    val DFTwentyOne = spark.table(hiveTableFlightsV)
      .withColumn("actual_duration_t",
      to_timestamp(col("actual_duration")))
      .withColumn("ad_min",
        minute(col("actual_duration_t")) + hour(col("actual_duration_t")) * 60)
      .groupBy("departure_city")
      .agg(avg("ad_min").alias("avg_minutes"))
      .filter(col("avg_minutes") > 180)
      .orderBy(col("avg_minutes").desc)
      .withColumn("id", lit(21))
      .withColumnRenamed("departure_city", "response")
      .select("id", "response")
      .limit(5)
      .orderBy("response")

    val DFFinal = DFOne.union(DFTwo)
      .union(DFThree)
      .union(DFFour)
      .union(DFFive)
      .union(DFSix)
      .union(DFSeven)
      .union(DFEight)
      .union(DFNine)
      .union(DFTen)
      .union(DFEleven)
      .union(DFTwelve)
      .union(DFThirteen)
      .union(DFFourteen)
      .union(DFFifteen)
      .union(DFSixteen)
      .union(DFSeventeen)
      .union(DFEighteen)
      .union(DFNineteen)
      .union(DFTwenty)
      .union(DFTwentyOne)

    DFFinal.write.mode("overwrite").saveAsTable(hiveTableResults)
    spark.stop()
  }
}

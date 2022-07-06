import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}
import java.sql.DriverManager
import java.util.Properties


object MainAppEl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("yarn")
      .appName("MainAppEl_mvchernov")
      .config("spark.hadoop.hive.exec.dynamic.partition", "true")
      .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    Class.forName("org.postgresql.Driver")

    val url = "jdbc:postgresql://url.prod.dmp.url.ru:5448/demo"
    val props = new Properties()
    props.setProperty("user", args(0))
    props.setProperty("password", args(1))
    props.setProperty("driver", "org.postgresql.Driver")
    val con = DriverManager.getConnection(url, props)

    val pgTableSeats = "bookings.seats"
    val hiveTableSeats = "school_de.seats_mvchernov"
    val pgSeatsDF = spark.read.jdbc(url, pgTableSeats, props)
    pgSeatsDF.write.format("hive").mode("overwrite").saveAsTable(hiveTableSeats)

    val pgTableFlights = "bookings.flights_v"
    val hiveTableFlights = "school_de.flights_v_mvchernov"
    val pgFlightsDF = spark.read.jdbc(url, pgTableFlights, props)
    val pgFlightsDFPart = pgFlightsDF
      .withColumn("actual_departure_date",
        to_date(col("actual_departure"))
      )

    pgFlightsDFPart.write
      .partitionBy("actual_departure_date")
      .format("hive")
      .mode("overwrite")
      .saveAsTable(hiveTableFlights)

    spark.stop()
  }
}

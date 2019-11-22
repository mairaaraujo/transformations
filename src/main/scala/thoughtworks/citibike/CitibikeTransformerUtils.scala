package thoughtworks.citibike

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object CitibikeTransformerUtils {
  private final val MetersPerFoot = 0.3048
  private final val FeetPerMile = 5280

  final val EarthRadiusInM: Double = 6371e3
  final val MetersPerMile: Double = MetersPerFoot * FeetPerMile


  implicit class StringDataset(val dataSet: Dataset[Row]) {

    val distance = (start: Long, end: Long) => {
      (end - start).toRadians
    }


    def computeDistances(spark: SparkSession) = {
      import spark.implicits._

      spark.udf.register("distance", distance)

      val R = 6372.8d  //radius in km

      val dLat = dataSet.withColumn("dLat", $"end_station_latitude" - $"start_station_latitude")
      val dLon = dLat.withColumn("dLon", $"end_station_longitude" - $"start_station_longitude")

      val a = dLon.withColumn("a",
          pow(sin(dLat.col("dLat") / 2), 2) + pow(sin(dLon.col("dLon") / 2), 2) * cos(dataSet.col("start_station_latitude"))
            * cos(dataSet.col("end_station_latitude")))

      val c = a.withColumn("c", lit(asin(a.col("a")) * 2))

      val withDistance = c.withColumn("distance", lit(c.col("c") * R))

      withDistance.drop("c").drop("a").drop("dLon").drop("dLat")

//      val datasetWithDistance =
//        dataSet.withColumn("distance",
//          dataSet.map(x => {
//
//            val dLat = distance((x("start_station_latitude")).asInstanceOf[Long], ((x("end_station_latitude"))).asInstanceOf[Long])
//            val dLon = distance((x("start_station_longitude")).asInstanceOf[Long], (x("end_station_longitude")).asInstanceOf[Long])
//
//
//            val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(dataSet.col("start_station_latitude")) * cos(dataSet.col("end_station_latitude"))
//            val c = 2 * asin(sqrt(a))
//
//            val distanceVal = R * c
//          })
    }
  }
}

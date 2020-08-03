package covidimpact

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.{Dataset, SparkSession, Row}

import scala.io.Source

object Main extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  val spark: SparkSession = SparkSession.
    builder().
    appName("Covid Impact").
//    config("", "").
    master("local").
    getOrCreate()

  val data = new CovidData(spark, debug = true)
  val processing = new CovidProcessing(spark, data)

  spark.close()
}

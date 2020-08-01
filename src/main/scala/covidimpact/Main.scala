package covidimpact

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SparkSession

import scala.io.Source

object Main extends App {

  val spark: SparkSession = SparkSession.
    builder().
    appName("Covid Impact").
//    config("", "").
    master("local").
    getOrCreate()

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  val data = new CovidData(spark)
  val tw = data.Twitter.load()

  spark.close()
}

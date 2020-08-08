package covidimpact

import org.apache.log4j.{ConsoleAppender, Level, Logger}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Main extends App {

  // Runtime parameters
  val TWEETS_LOCATION = "./save/tweetsds"
  val DF_LOCATION  = "./save/masterdf"
  // (T,_): Reload from disk; (F,F): Reload everything; (F,T): Reload a fraction
  val FROM_DISK = false
  val DEBUG  = false

  // Logging
  val myLogger = Logger.getLogger("Report")
  myLogger.setLevel(Level.DEBUG)
  myLogger.addAppender(new ConsoleAppender())
  myLogger.info("Initialize Report logger")

  // Fire up Apache Spark
  val spark: SparkSession = SparkSession.builder().
    appName("Covid Impact").
    master("local").
    getOrCreate()
  myLogger.debug("############## Spark Session online ##############")

  import spark.implicits._

  // Calling processing object
  lazy val processing = new CovidProcessing(spark, new CovidData(spark, DEBUG))

  // Loading lazily (mainly for performance/debugging purposes) both datasets
  lazy val masterDF: DataFrame = Util.retrieveFromDisk[Row](DF_LOCATION,
    processing.joinAll(), RowEncoder(Util.dfSchema), logMessage = "MasterDF")

  lazy val tweetsDS: Dataset[TwitterDay] = Util.retrieveFromDisk[TwitterDay](TWEETS_LOCATION,
    processing.covidTopThousand, newProductEncoder, logMessage = "TweetsDS")


  // Doing analytics
  Util.time( new CovidAnalysis(spark).execute(masterDF, tweetsDS), "APPLICATION")
  spark.close()
}

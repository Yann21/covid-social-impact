package covidimpact

import java.io.File

import org.apache.log4j.{ConsoleAppender, FileAppender, Level, LogManager, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.types._
import scala.collection.parallel.ParSeq
import scala.collection.parallel.mutable.ParArray
import scala.reflect.ClassTag
import scala.reflect.io.Directory

/**
 * JUnit testing... But how?
 * Create some phony dataset and make them go through every method
 */
object Main extends App {
  /** Runtime parameters */
  val TWEETS_LOCATION = "./save/tweetsds"
  val DF_LOCATION  = "./save/masterdf"
  val FROM_DISK = true
  val DEBUG  = true
  val LOG = "Report"

  /** Logging */
  val myLogger = Logger.getLogger(LOG)
  myLogger.setLevel(Level.DEBUG)
  myLogger.addAppender(new ConsoleAppender())
  myLogger.info("Initialize Report logger")

  /** Fire up spark */
  val spark: SparkSession = SparkSession.
    builder().
    appName("Covid Impact").
    master("local").
    getOrCreate()

  import spark.implicits._

  def saveToDisk[T](ds: Dataset[T], filepath: String): Unit = {
    val dir= new Directory(new File(filepath))
    val debugString = if (dir.deleteRecursively()) "Save directory successfully deleted" else "Directory already deleted"

    Logger.getLogger(LOG).debug(debugString)
    Logger.getLogger(LOG).debug("Saving checkpoint...")
    ds.write.format("parquet").save(filepath)
    ds show 5
  }
  def loadFromDisk[T <: Serializable: Encoder](filepath: String): Dataset[T] = {
    Logger.getLogger(LOG).debug("Loading from disk...")
    val df: Dataset[T] = spark.read.parquet(filepath).orderBy("date").as[T]

    Logger.getLogger(LOG).info(s"${df.count} many entries")
    df show 5; df
  }

  /** Abstracting the need for checkpointing and retrieving from disk */
  def retrieveFromDisk[T <: Serializable](location: String, retrieval: () => Dataset[T],
                          encoding: Encoder[T], logMessage: String = ""): Dataset[T] =
    if (FROM_DISK) loadFromDisk[T](location)(encoding)
    else {
      val ds: Dataset[T] = retrieval()
      saveToDisk(ds, location); ds
    }

  val dfSchema = StructType(Seq(
    StructField("date",    DateType), StructField("confirmed", IntegerType, true), StructField("recovered", IntegerType, true),
    StructField("deaths",  IntegerType, true), StructField("workplaces", DoubleType, true), StructField("transit", DoubleType),
    StructField("walking", DoubleType), StructField("residential", DoubleType), StructField("parks", DoubleType),
    StructField("grocery", DoubleType), StructField("driving", DoubleType), StructField("stations", DoubleType),
    StructField("retail",  DoubleType), StructField("waze",    DoubleType)
  ))


  val processing = new CovidProcessing(spark, new CovidData(spark, DEBUG = DEBUG))

  def masterDF: DataFrame = retrieveFromDisk[Row](DF_LOCATION,
    () => processing.joinAll(), RowEncoder(dfSchema), logMessage = "MasterDF")

  def tweetsDS: Dataset[TwitterDay] = retrieveFromDisk[TwitterDay](TWEETS_LOCATION,
    () => processing.covidTopThousand, newProductEncoder, logMessage = "TweetsDS")

  val analysis = new CovidAnalysis(masterDF, tweetsDS)

  spark.close()
}

package covidimpact

import java.io.File
import java.util.logging.Logger

import covidimpact.Main._

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoder}

import scala.reflect.io.Directory

object Util {
  def time[T](block: => T, logMessage: String = ""): T = {
    val t0 = System.nanoTime()
    println("")
    Logger.getLogger("Report").info(s"Starting timer $logMessage...")
    val res = block
    val t1 = System.nanoTime()
    Logger.getLogger("Report").info(s"Time elapsed: ${(t1 - t0)/1e6}ms $logMessage")
    res
  }

  def saveToDisk[T](ds: Dataset[T], filepath: String): Unit = {
    val dir= new Directory(new File(filepath))
    val debugString = if (dir.deleteRecursively()) "Save directory successfully deleted" else "Directory already deleted"

    myLogger.debug(debugString)
    myLogger.debug("Saving checkpoint...")
    ds.write.format("parquet").save(filepath)
    ds show 5
  }

  def loadFromDisk[T <: Serializable: Encoder](filepath: String): Dataset[T] = {
    myLogger.debug("Loading from disk...")
    val df: Dataset[T] = spark.read.parquet(filepath).orderBy("date").as[T]

    myLogger.info(s"${df.count} many entries")
    df show 5; df
  }

  /** Abstracting the need for checkpointing and retrieving from disk */
  def retrieveFromDisk[T <: Serializable](location: String, retrieval: => Dataset[T],
                                          encoding: Encoder[T], logMessage: String = ""): Dataset[T] =
    if (FROM_DISK) loadFromDisk[T](location)(encoding)
    else {
      val ds: Dataset[T] = Util.time(retrieval)
      if (!DEBUG) saveToDisk(ds, location)
      ds
    }

  val dfSchema: StructType = StructType(Seq(
    StructField("date",    DateType), StructField("confirmed", IntegerType, true), StructField("recovered", IntegerType, true),
    StructField("deaths",  IntegerType, true), StructField("workplaces", DoubleType, true), StructField("transit", DoubleType),
    StructField("walking", DoubleType), StructField("residential", DoubleType), StructField("parks", DoubleType),
    StructField("grocery", DoubleType), StructField("driving", DoubleType), StructField("stations", DoubleType),
    StructField("retail",  DoubleType), StructField("waze",    DoubleType)
  ))

}

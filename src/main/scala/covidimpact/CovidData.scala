package covidimpact

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.io.Source

/**
 * Data object to load the data into spark and access
 * it for further processing and analysis.
 *
 * @param spark Handle to spark APIs
 */
case class TwitterEntry(
                         date: java.sql.Date,
                         n: Int,
                         grams: GramCount
                       )

case class GramCount (
                       gram: String,
                       count: String
                     )
class CovidData(spark: SparkSession) {
  trait DataField {
    /** Chains the operations and exposes the dataset */
    def read(): Unit
    /** Load the files */
    def load(): Unit
    /** Droplines FilterByData */
    def clean(): Unit
  }

  val pathToRes: String = "src/main/resources/"
  type Country = String
  type Gram = Seq[String]

//  val tweetsData: Dataset[Twitter.TwitterEntry] = ???
//  val johnsHopkinsData: Dataset[JohnsHopkins.JHEntry] = ???
//  val mobilityData: Dataset[Mobility.MobilityEntry] = ???

  import spark.implicits._

  /**
   * @see https://ref.com
   */
  object Twitter extends DataField {
    val twitterDateFormat = new SimpleDateFormat("yyyy-mm-dd")
    val schema= Encoders.product[GramCount].schema

    def load(): Unit = {
      val dir: File = new File(pathToRes ++ "covidimpact/core/DailyTweets")
      assert(dir.exists && dir.isDirectory)
      val listOfFiles: Array[File] = dir.listFiles

      def includeMetaInformation(file: File): Dataset[TwitterEntry] = {
        // Filename format: "yyyy-mm-dd_cccssss{n-grams}.csv"
        //                  "0000-00-00_0123456(7)_____(4)321
        print("Checkpoint")
        println(file.getName)
        val (date: String, string: String) = file.getName.split("_") match {
          case Array(date, string) => (date, string)
        }
        print("Checkpoint Other")
        val nGrams: Int = string.substring(7).dropRight(4) match {
            case "terms" => 1
            case "bigrams" => 2
            case "trigrams" => 3
          }

        val df: DataFrame = spark.read.option("header", "true").schema(schema).csv(file.getPath)
        val ds: Dataset[GramCount] = df.as[GramCount]
        val timedDs= ds map( gramCount =>
          TwitterEntry(java.sql.Date.valueOf(date), nGrams, gramCount) )

        timedDs
      }

      @scala.annotation.tailrec
      def mergeAllDataset(ds: Dataset[TwitterEntry],
                        files: Array[File]): Dataset[TwitterEntry] =
        if (files.isEmpty) ds
        else mergeAllDataset(ds.union(includeMetaInformation(files.head)), files.tail)

      mergeAllDataset(spark.emptyDataset[TwitterEntry], listOfFiles)
    }
    def clean(): Unit = ???

    def read(): Unit = ???
  }

  object JohnsHopkins extends DataField {
    case class JHEntry(a: String)

    def read(): Unit = ???

    def load(): Unit = ???

    def clean(): Unit = ???
  }

  object Mobility extends DataField {
    case class MobilityEntry(a: String)

    def read(): Unit = ???

    def load(): Unit = ???

    def clean(): Unit = ???
  }




  /**
   * Data.Additional contains a set of complementary
   * data consisting of lookup tables for specific one-time
   * events such as the date of lockdown (if any) by localities.
   */
  class Additional {
    /** @see https://www.kaggle.com/jcyzag/covid19-lockdown-dates-by-country */
    val lookupLockdownDatesLookup: Map[Country, Date] = {
      val it = Source.fromFile(pathToRes ++ "covidimpact/aux/countryLockdowndates.csv").getLines
      it.next() // Ignore header
      val dateParser = new SimpleDateFormat("dd/mm/yyyy")

      val countryLockdownDatePairs: Iterator[(String, Date)] =
        for (line <- it)
          yield {
            line.split(",", -1) match {
              case Array(count, prov, d, _, _) => {
                val province: Option[String] = if (prov.isEmpty) None else Some (prov)
                val date: Date = if (d.isEmpty) new Date(0L) else dateParser.parse(d)
                val identifier: String = List(Some(count), province).flatten.mkString("_")

                (identifier, date)
              }
                // Edge Case, see: "Korea, South" in CSV
              case sk => ("South Korea", dateParser.parse("22/02/2020"))
            }
          }

      countryLockdownDatePairs.toMap
    }

  }

}

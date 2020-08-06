package covidimpact

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.io.Source

/**
 * Data object to load the data into spark and access
 * it for further processing and analysis.
 *
 * Available datasets:
 *  - Waze Mobility
 *  - Google Mobility
 *  - JH Confirmed
 *  - JH Deaths
 *  - JH Recovered
 *  - Twitter n-grams
 *
 * @param spark Handle to spark APIs
 */
class CovidData(spark: SparkSession, DEBUG: Boolean = false) {
  val pathToRes: String = "src/main/resources/"
  type Country = String

  import spark.implicits._

  /**
   * @see https://ref.com
   *
   * Using the top 1000 {1-3}grams in tweets.
   */
  object Twitter {
    val tweetsDs: Dataset[TwitterEntry] = load()

    def load(): Dataset[TwitterEntry] = {
      val schema = Encoders.product[RawTwitterEntry].schema
      val dir: File = new File(pathToRes ++ "covidimpact/core/DailyTweets")
      assert(dir.exists && dir.isDirectory)
      val listOfFiles: Array[File] =
        if (DEBUG) dir.listFiles take 5
        else dir.listFiles

      /**
       * Note the inversion of bigrams and trigrams due to the incorrect
       * original file naming.
       *
       * File format: "yyyy-mm-dd_cccssss{n-grams}.csv"
       *              "0000-00-00_0123456(7)_____(4)321"
       */
      def extractMetaInfoFromFileName(file: File): (java.sql.Date, Int) = {
        val (date: java.sql.Date, string: String) = file.getName.split("_") match {
          case Array(d, string) => (java.sql.Date.valueOf(d), string)
        }
        val nGram: Int = string.substring(7).dropRight(4) match {
          case "terms" => 1
          case "bigrams" => 3
          case "trigrams" => 2
        }

        (date, nGram)
      }

      def parseCSV(file: File): Dataset[TwitterEntry] = {
        if (DEBUG) print(".")
        val meta = extractMetaInfoFromFileName(file)

        val ds: Dataset[RawTwitterEntry] =
          spark.read.option("header", "true").
          schema(schema).csv(file.getPath).
          as[RawTwitterEntry]

        val timedDs= ds map(
          rawEntry => {
            val (a, b, c) = rawEntry.gram.split(" ") match {
              case Array(a, b, c) => (a, b, c)
              case Array(a, b) => (a, b, "")
              case Array(a) => (a, "", "")
            }
            TwitterEntry(
              meta._1, meta._2, a, b, c, rawEntry.counts.getOrElse(0)
            )
          })

        timedDs
        }

      @scala.annotation.tailrec
      def mergeAll(ds: Dataset[TwitterEntry],
                          files: Array[File]): Dataset[TwitterEntry] =
        if (files.isEmpty) ds
        else mergeAll(ds union parseCSV(files.head), files.tail)
      Logger.getLogger("Report").debug("Loading twitter data.")
      val merged = mergeAll(spark.emptyDataset[TwitterEntry], listOfFiles)

      Logger.getLogger("Report").info(s"${merged.count.toString} twitter entries")
      merged
    }
  }


  /**
   * Using JH time series for confirmed, deaths and recovered per country.
   */
  object JohnsHopkins {
    val dir: File = new File(pathToRes ++ "covidimpact/core/CSSECovidData/csse_covid_19_time_series")
    assert(dir.exists && dir.isDirectory)
    val listOfFiles: Array[File] = Array("/confirmed_global.csv", "/deaths_global.csv", "/recovered_global.csv").
      map( timeSeries => new File(dir.getPath ++ timeSeries) )

    val confirmedDf: DataFrame = spark.read.option("inferSchema", "true").option("header", "true").csv(listOfFiles(0).getPath)
    val deathsDf:    DataFrame = spark.read.option("inferSchema", "true"). option("header", "true").csv(listOfFiles(1).getPath)
    val recoveredDf: DataFrame = spark.read.option("inferSchema", "true").option("header", "true").csv(listOfFiles(2).getPath)
  }


  object Mobility {
    val dir: File = new File(pathToRes ++ "covidimpact/core/MobilityData")
    assert(dir.exists && dir.isDirectory)
    val listOfFiles: Array[File] = Array("/summary_report_regions.csv", "/waze_mobility.csv").
      map( timeSeries => new File(dir.getPath ++ timeSeries))

    def typeSummary(df: DataFrame): Dataset[SummaryEntry] = {
      df map ( row => SummaryEntry(
        row.getAs[Country]("country"),
        row.getAs[String]("region"),
        java.sql.Date.valueOf(row.getAs[String]("date")),
        row.getAs[Double]("retail and recreation"),
        row.getAs[Double]("grocery and pharmacy"),
        row.getAs[Double]("parks"),
        row.getAs[Double]("transit stations"),
        row.getAs[Double]("workplaces"),
        row.getAs[Double]("residential"),
        row.getAs[Double]("driving"),
        row.getAs[Double]("transit"),
        row.getAs[Double]("walking")
      ))
    }
    def typeWaze(df: DataFrame): Dataset[WazeEntry] = {
      df map (row => WazeEntry(
        row.getAs[Country]("country"),
        row.getAs[String]("city"),
        row.getAs[String]("geo_type"),
        java.sql.Date.valueOf(row.getAs[String]("date")),
        row.getAs[Double]("driving_waze")
      ))
    }

    val summaryDf: Dataset[SummaryEntry] = typeSummary(
      spark.read.option("inferSchema", "true").option("header", "true").csv(listOfFiles(0).getPath))
    val wazeDf: Dataset[WazeEntry] = typeWaze(
      spark.read.option("inferSchema", "true").option("header", "true").csv(listOfFiles(1).getPath))
  }



  /**
   * Data.Additional contains a set of complementary
   * data consisting of lookup tables for specific one-time
   * events such as the date of lockdown (if any) by localities.
   */
  object Additional {
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

    val englishWords: DataFrame = spark.read.text(pathToRes ++ "covidimpact/aux/en_words.txt")
    val stopWords:    DataFrame = spark.read.text(pathToRes ++ "covidimpact/aux/stopwords.txt")
  }

}

case class SummaryEntry (
  country:     String,
  region:      String,
  date:        java.sql.Date,
  retail:      Double,
  grocery:     Double,
  parks:       Double,
  stations:    Double,
  workplaces:  Double,
  residential: Double,
  driving:     Double,
  transit:     Double,
  walking:     Double
)
case class WazeEntry (
  country:      String,
  city:         String,
  geo_type:     String,
  date:         java.sql.Date,
  driving_waze: Double
)

case class TwitterEntry (
  date:     java.sql.Date,
  n:        Int,
  gram1:    String,
  gram2:    String,
  gram3:    String,
  occ:      Int
)
case class RawTwitterEntry(
  gram:     String,
  counts:    Option[Int]
)
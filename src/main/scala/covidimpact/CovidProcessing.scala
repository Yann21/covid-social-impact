package covidimpact

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.Logger
import org.apache.spark.ml.feature.{Normalizer, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * TODO:
 *    ° Twitter Data
 *  - Cloud association with covid
 *
 *  200,000 words
 *  138 countries
 *  300000 twitter entries
 *  Featurization
 */
class CovidProcessing(spark: SparkSession, data: CovidData) {
  import spark.implicits._

  /** Object I: Johns Hopkins Data Processing
   *  Tangible information and primary predictive feature.
   *
   *  - Aggregating over countries
   *  - Transposing DataFrame so there are only 2 columns (date, figure)
   */

  /**
   * 1. DataFrame: Remove metadata keep `Schema`
   * 2. RDD: Sum up rows
   * 3. Row: World Series
   * 4. RDD: Parallelize Row into RDD
   * 5. DataFrame: Reconstruct DF with previous `Schema`
   *
   * @param johnsHopkins
   * @return Summing over all contries
   */
  def dailySumRowsOverAllCountries(johnsHopkins: DataFrame): DataFrame = {
    val timeSeries: DataFrame = johnsHopkins.drop("Province/State", "Country/Region", "Lat", "Long")
    val summedRow: Row = timeSeries.rdd.
      reduce((r1, r2) =>
        Row.fromSeq( (r1.toSeq zip r2.toSeq).map(a => a._1.asInstanceOf[Int]
                                                    + a._2.asInstanceOf[Int]) )
      )

    val myRDD: RDD[Row] = spark.sparkContext.parallelize(Seq(summedRow))
    val worldSeries: DataFrame = spark.createDataFrame(myRDD, timeSeries.schema)

    worldSeries
  }

  /**
   * Dirty transposition hack. Move along...
   * @param df
   * @return
   */
  def transposeDF(df: DataFrame): DataFrame = {
    import java.sql.Date

    val schemaBefore = df.schema.map(_.name.asInstanceOf[String])
    val datesAndFigures: Seq[(String, Any)] = schemaBefore zip df.head.toSeq

    val pairSchema = StructType(Array(
      StructField("date", DateType, nullable = false),
      StructField("value", IntegerType, nullable = false)
    ))

    val myRows: Seq[Row] = datesAndFigures.map(pair => Row.fromSeq(Seq(new Date(new SimpleDateFormat("MM/dd/yy").parse(pair._1).getTime),
                                                                       pair._2.asInstanceOf[Int])))
    val myRDD: RDD[Row] = spark.sparkContext.parallelize(myRows)
    spark.createDataFrame(myRDD, pairSchema)
  }

  private val confirmed: DataFrame          = data.JohnsHopkins.confirmedDf.cache()
  private val recovered: DataFrame          = data.JohnsHopkins.recoveredDf.cache()
  private val deaths:    DataFrame          = data.JohnsHopkins.deathsDf.cache()

  private def colConfirmed = (transposeDF _ compose dailySumRowsOverAllCountries)(confirmed).
    withColumnRenamed("value", "confirmed")
  private def colRecovered = (transposeDF _ compose dailySumRowsOverAllCountries)(recovered).withColumnRenamed(
    "value", "recovered")
  private def colDeaths    = (transposeDF _ compose dailySumRowsOverAllCountries)(deaths).withColumnRenamed(
    "value", "deaths")


  /** Object II: Mobility data from Google and Ways
   * - Averaging response of all countries (Ways and Google)
   * - Reformatting google data
   */

  def averageDailyWazeMobility(waze: Dataset[WazeEntry]): DataFrame =
    waze.groupBy("date").avg("driving_waze").withColumnRenamed("avg(driving_waze)", "waze")

  def averageDailyGoogleMobility(google: Dataset[SummaryEntry]): DataFrame = {
    google.groupBy($"date").agg(Map(
      "retail"   -> "avg",
      "grocery"  -> "avg",
      "parks"    -> "avg",
      "stations" -> "avg",
      "workplaces" -> "avg",
      "residential" -> "avg",
      "driving"  -> "avg",
      "transit"  -> "avg",
      "walking"  -> "avg"
    )).
      withColumnRenamed("avg(retail)",   "retail").
      withColumnRenamed("avg(grocery)",  "grocery").
      withColumnRenamed("avg(parks)",    "parks").
      withColumnRenamed("avg(stations)", "stations").
      withColumnRenamed("avg(workplaces)", "workplaces").
      withColumnRenamed("avg(residential)", "residential").
      withColumnRenamed("avg(driving)",  "driving").
      withColumnRenamed("avg(transit)",  "transit").
      withColumnRenamed("avg(walking)",  "walking")
  }

  def assembleGoogle(google: DataFrame): DataFrame = {
    val featureColumns = Array("retail", "grocery", "parks", "stations", "workplaces",
      "residential", "driving", "transit", "walking")
    val assembler = new VectorAssembler().
      setInputCols(featureColumns).
      setOutputCol("google")

    // Country, Region, Date, Feature Vector
    val assembled: DataFrame = assembler.transform(google).
      drop("retail", "grocery", "parks", "stations", "workplaces", "residential",
        "driving", "transit", "walking")

//    assembled
    google
  }

  private val googleDS: Dataset[SummaryEntry] = data.Mobility.summaryDf.cache()
  private val wazeDS:   Dataset[WazeEntry]    = data.Mobility.wazeDf.cache()

  private def google: DataFrame = (assembleGoogle _ compose averageDailyGoogleMobility)(googleDS)
  private def waze: DataFrame = averageDailyWazeMobility(wazeDS)


  /** Object III: Twitter data
   * - Filtering tweets for english words
   * - Removing stop words
   * - Creating a proxy signal for the mental awareness of covid by summing up
   *   all covid-19 mentions
   * - Keeping most tweeted words per day for further NLP
   *
   */

  private val coronabc: DataFrame = spark.sparkContext.
    parallelize(Seq("covid", "covid19", "coronavirus", "virus", "pandemic", "covid_19", "corona")).toDF
  private val stopWords: DataFrame = data.Additional.stopWords.cache()

  def filterTweets(tweets: Dataset[TwitterEntry]): Dataset[TwitterEntry] = {
    val countTweets = tweets.count
    val filteredEnglish = tweets.join( englishWords,
      (tweets("gram1") === englishWords("value")) &&
        (tweets("gram2") === englishWords("value") || tweets("gram2") === "") &&
          (tweets("gram3") === englishWords("value") || tweets("gram3") === ""), "leftsemi").as[TwitterEntry]
    val countFilteredEnglish = filteredEnglish.count

    val filteredStopWords = filteredEnglish.join(stopWords,
      (filteredEnglish("gram1") === stopWords("value")) &&
        (filteredEnglish("gram2") === stopWords("value") || filteredEnglish("gram2") === "") &&
          (filteredEnglish("gram3") === stopWords("value") || filteredEnglish("gram3") === ""), "left_anti").as[TwitterEntry]
    val countFilteredStopWords = filteredStopWords.count

    val filterGram = filteredStopWords.filter($"gram1" =!= "gram")

    Logger.getLogger("Report").info(s"Removed ${countTweets - countFilteredEnglish} non english words")
    Logger.getLogger("Report").info(s"Removed ${countFilteredEnglish - countFilteredStopWords} english stop words")
    Logger.getLogger("Report").info(s"Remaining entries: $countFilteredStopWords")
    filterGram.orderBy($"occ".desc)
  }

  def covidDailySignal(tweets: Dataset[TwitterEntry]): Dataset[CovidSignal] = {
    val covidMentions = tweets.join(coronabc, tweets("gram1") === coronabc("value"), "leftsemi")
    covidMentions.groupBy("date").sum("occ").orderBy($"date").
      toDF("date", "mentions").as[CovidSignal]
  }

  def covidTwitterWords(tweets: Dataset[TwitterEntry]): Dataset[TwitterDay] = {
    tweets.groupByKey(_.date).mapGroups( // foldLeft
      (dates, it) => TwitterDay(dates, it.map(_.gram1).toList
    ))
  }

  private def englishWords: DataFrame      = (data.Additional.englishWords union coronabc).cache()
  private val tweets = data.Twitter.tweetsDs.cache()
  private val filtered: Dataset[TwitterEntry] = filterTweets(tweets).cache()

  private def covidTwitterSignal: Dataset[CovidSignal] = covidDailySignal(filtered)
  /** NLP Input */
  def covidTopThousand: Dataset[TwitterDay] = covidTwitterWords(filtered)


  /**
   * Combining Google mobility values, ways numbers, johns hopkins' recovered, deaths and confirmed,
   * and lastly the twitter hand crafted signal.
   *
   * @return masterDF
   */
  def joinAll(): DataFrame = {
    colConfirmed.
      join(covidTwitterSignal,  Seq("date"), "outer").
      join(colRecovered,  Seq("date"), "outer").
      join(colDeaths,  Seq("date"), "outer").
      join(google,  Seq("date"), "outer").
      join(waze, Seq("date"), "outer").
      orderBy($"date")
  }
}

case class CovidSignal (
  date: java.sql.Date,
  mentions: Double
)

case class TwitterDay (
  date: java.sql.Date,
  topWords: List[String]
)
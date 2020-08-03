package covidimpact

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.ml.feature.{Normalizer, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * TODO:
 *  - Clip dates
 *  - Aggregate by country
 *  - Remove missing values if any
 *  _
 *    ° Mobility
 *  - Normalize waze value
 *  _
 *    ° Twitter Data
 *  - Cloud association with covid
 *
 *  138 countries
 *  300000 twitter entries
 *  Featurization
 */
class CovidProcessing(spark: SparkSession, data: CovidData) {
  import spark.implicits._

  val coronabc: DataFrame = spark.sparkContext.
    parallelize(Seq("covid", "covid19", "coronavirus", "virus", "pandemic", "covid_19", "corona")).toDF

  //  def lockdowndata: Map[data.Country, Date] = data.Additional.lookupLockdownDatesLookup
  def tweets: Dataset[TwitterEntry] = data.Twitter.tweetsDs
  def _englishWords: DataFrame      = data.Additional.englishWords
  def stopWords:    DataFrame       = data.Additional.stopWords


  val englishWords: DataFrame = _englishWords union coronabc

  def joinAll(tweets: Dataset[CovidSignal], google: DataFrame, waze: DataFrame,
              confirmed: DataFrame, recovered: DataFrame, deaths: DataFrame
             ): DataFrame = {
    tweets join google join waze join confirmed join recovered join deaths
  }
  joinAll(covidTwitterSignal, google, waze, confirmed, recovered, deaths)



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
  def sumRowsOverAllCountries(johnsHopkins: DataFrame): DataFrame = {
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
   * Dirty hack. Move along...
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

    val myRows: Seq[Row] = datesAndFigures.map(pair => Row.fromSeq(Seq(new Date(new SimpleDateFormat("dd/mm/yy").parse(pair._1).getTime),
                                                                       pair._2.asInstanceOf[Int])))
    val myRDD: RDD[Row] = spark.sparkContext.parallelize(myRows)
    spark.createDataFrame(myRDD, pairSchema)
  }

  val confirmed: DataFrame          = data.JohnsHopkins.confirmedDf.cache()
  val recovered: DataFrame          = data.JohnsHopkins.recoveredDf.cache()
  val deaths:    DataFrame          = data.JohnsHopkins.deathsDf.cache()

  def colConfirmed = (transposeDF _ compose sumRowsOverAllCountries)(confirmed)
  def colrecovered = (transposeDF _ compose sumRowsOverAllCountries)(recovered)
  def coldeaths    = (transposeDF _ compose sumRowsOverAllCountries)(deaths)




  def averageDailyWazeMobility(waze: Dataset[WazeEntry]): DataFrame =
    waze.groupBy("date").avg("driving_waze").as("waze")

  def averageDailyGoogleMobility(google: Dataset[SummaryEntry]): DataFrame = {
    google.groupBy($"date").agg(Map(
      "retail" -> "avg",
      "grocery" -> "avg",
      "parks" -> "avg",
      "stations" -> "avg",
      "workplaces" -> "avg",
      "residential" -> "avg",
      "driving" -> "avg",
      "transit" -> "avg",
      "walking" -> "avg"
    )).
      withColumnRenamed("avg(retail)", "retail").
      withColumnRenamed("avg(grocery)", "grocery").
      withColumnRenamed("avg(parks)", "parks").
      withColumnRenamed("avg(stations)", "stations").
      withColumnRenamed("avg(workplaces)", "workplaces").
      withColumnRenamed("avg(residential)", "residential").
      withColumnRenamed("avg(driving)", "driving").
      withColumnRenamed("avg(transit)", "transit").
      withColumnRenamed("avg(walking)", "walking")
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

    assembled
  }

  val googleDS: Dataset[SummaryEntry] = data.Mobility.summaryDf.cache()
  val wazeDS:   Dataset[WazeEntry]    = data.Mobility.wazeDf.cache()
  def google: DataFrame = (assembleGoogle _ compose averageDailyGoogleMobility)(googleDS)
  def waze: DataFrame = averageDailyWazeMobility(wazeDS)



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

    println(s"Removed ${countTweets - countFilteredEnglish} non english words")
    println(s"Removed ${countFilteredEnglish - countFilteredStopWords} english stop words")
    println(s"Remaining entries: $countFilteredStopWords")
    filterGram.orderBy($"occ".desc)
  }

  def covidDailySignal(tweets: Dataset[TwitterEntry]): Dataset[CovidSignal] = {
    val covidMentions = tweets.join(coronabc, tweets("gram1") === coronabc("value"), "leftsemi")
    covidMentions.groupBy("date").sum("occ").orderBy($"date").
      toDF("date", "mentions").as[CovidSignal]
  }

  englishWords.cache()
  tweets.cache()
  def covidTwitterSignal: Dataset[CovidSignal] = (covidDailySignal _ compose filterTweets)(tweets)
}

case class CovidSignal (
  date: java.sql.Date,
  mentions: Double
)
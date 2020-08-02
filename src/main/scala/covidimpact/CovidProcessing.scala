package covidimpact

import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * TODO:
 *  - Clip dates
 *  - Aggregate by country
 *  - Remove missing values if any
 *  - Normalize waze value
 *
 *  138 countries
 */
class CovidProcessing(spark: SparkSession, data: CovidData) {
  import spark.implicits._

  def tweets: Dataset[TwitterEntry] = data.Twitter.tweetsDs
  def google: Dataset[SummaryEntry] = data.Mobility.summaryDf
  def waze:   Dataset[WazeEntry]    = data.Mobility.wazeDf
  def confirmed: DataFrame          = data.JohnsHopkins.confirmedDf
  def recovered: DataFrame          = data.JohnsHopkins.recoveredDf
  def deaths:    DataFrame          = data.JohnsHopkins.deathsDf

//  def lockdowndata: Map[data.Country, Date] = data.Additional.lookupLockdownDatesLookup

//  waze.cache().createOrReplaceTempView("waze") // Problem of amplitude
//  google.cache().createOrReplaceTempView("google").cache()
  tweets.cache().createOrReplaceTempView("tweets")

  def keepEnglish(tweets: Dataset[TwitterEntry]): Dataset[TwitterEntry] = {
    ???
//    val words = data.Additional.englishWords
//    val originalLength = tweets.count
//    val filtered = tweets filter $"n".isin(words)
//    filtered
  }
//  keepEnglish(tweets).show


  def removeStopWords(corpus: Dataset[TwitterEntry]): Dataset[TwitterEntry] = {
    ???
  }
}

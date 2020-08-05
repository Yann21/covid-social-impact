package covidimpact

import org.apache.log4j.Logger
import org.apache.spark.ml.feature.{FeatureHasher, VectorAssembler}
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
 * TODO:
 *  - Cross correlation
 *  - LDA?
 *  - ...
 */
class CovidAnalysis(data: DataFrame, tweets: Dataset[TwitterDay]) {


  val assembler = new VectorAssembler().
    setInputCols(Array("confirmed", "mentions", "deaths", "recovered", "waze", "workplaces", "transit", "walking",
                       "residential", "parks", "grocery", "driving", "stations", "retail", "waze")).
    setOutputCol("features")
  assembler.setHandleInvalid("skip")
  val featurized = assembler.transform(data).select("date", "features")

  def correlationStatistic(df: DataFrame, spearman: Boolean = false): Matrix = {
    val Row(mat: Matrix) =
      if (spearman) Correlation.corr(df, "features", "spearman").head
      else Correlation.corr(df, "features").head

    Logger.getLogger("Report").
      info(s"Cross Correlation Matrix: (${mat.numRows}x${mat.numRows})")
    mat
  }

  val matrix = correlationStatistic(featurized, spearman = false)
  println(matrix)


  /**
   *
   * TODO:
   *   LDA + Regression trees (not decision but close)
   *   Model topics
   *   HPC Production
   *   Report
   *   Stop
   *
   */

}

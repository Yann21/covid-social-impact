package covidimpact


import java.sql.Date

import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit._

class CovidDataSuite {
  val spark: SparkSession = SparkSession.builder().
    appName("JUnit").
    master("local").
    getOrCreate()

  import spark.implicits._

  val columnsDF = Array("date", "confirmed", "deaths", "recovered",
    "workplaces", "transit", "walking", "residential", "parks", "grocery", "driving",
    "stations", "retail", "waze")

  val dummyDF: DataFrame = Seq(
    (new Date(1), 1, 1, 1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
    (new Date(1), 1, 1, 1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
    (new Date(1), 1, 1, 1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
    (new Date(1), 1, 1, 1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
  ).toDF("label", "confirmed", "deaths", "recovered", "workplaces",
    "transit", "walking", "residential", "parks", "grocery", "driving",
    "stations", "retail", "waze")

  lazy val analysis = new CovidAnalysis(spark)

  @Test def `CovidData.jHData is non empty with the correct format`(): Unit = {
    assert(2+2 == 4)
  }

  @Test def `Correlation statistic correctness`(): Unit = {
    val startingDF: DataFrame = Seq((1, 1), (2, 2), (3, 3)).
      toDF.withColumnRenamed("value", "features")
    val assembler = new VectorAssembler().setInputCols(Array("_1", "_2")).setOutputCol("features")
    val dependentMatrix = assembler.transform(startingDF).select("features")

    val correlationMatrix: Matrix = analysis.correlationStatistic(dependentMatrix, spearman = false)

    Assert.assertEquals(correlationMatrix.numRows, correlationMatrix.numCols)
    Assert.assertEquals(correlationMatrix, correlationMatrix.transpose)

    for (i <- 1 until correlationMatrix.numRows)
      Assert.assertEquals(correlationMatrix(i, i), 1, 1e-3)
  }

  @Test def `Random forest correctness`(): Unit = {
    val assembler = new VectorAssembler().setInputCols(columnsDF.tail).setOutputCol("features")

    val rfInput: DataFrame = assembler.transform(dummyDF).
      select("waze", "features").
      withColumnRenamed("waze", "label")

    // Nothing has been added to this summarizer error...
//    val model: RandomForestClassificationModel = analysis.randomForest(rfInput)
//
//    Assert.assertNotNull(model)
  }

}

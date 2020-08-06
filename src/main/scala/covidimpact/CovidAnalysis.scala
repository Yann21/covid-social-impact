package covidimpact

import java.sql.Date

import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, FeatureHasher, IndexToString, StringIndexer, Tokenizer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * TODO:
 *  - Cross correlation
 *  - LDA?
 *  - ...
 */
class CovidAnalysis(spark: SparkSession) {


  def dataFrameAssembler(data: DataFrame): DataFrame = {
    val assembler = new VectorAssembler().
      setInputCols(Array("confirmed", "mentions", "deaths", "recovered", "waze", "workplaces", "transit", "walking",
        "residential", "parks", "grocery", "driving", "stations", "retail", "waze")).
      setOutputCol("features")
    assembler.setHandleInvalid("skip")
    val featurized = assembler.transform(data).select("date", "features")
    featurized
  }

  /**
   * Initial statistical analysis
   * @param df
   * @param spearman
   * @return
   */
  def correlationStatistic(df: DataFrame, spearman: Boolean = false): Matrix = {
    val Row(mat: Matrix) =
      if (spearman) Correlation.corr(df, "features", "spearman").head
      else Correlation.corr(df, "features").head

    Logger.getLogger("Report").
      info(s"Cross Correlation Matrix: (${mat.numRows}x${mat.numRows})")
    mat
  }



  /**
   *
   * TODO:
   *   HPC Production
   *   Report
   *   Stop
   *
   */

    import spark.implicits._

  def tokenizer(dataset: Dataset[TwitterDay]): DataFrame = {
    Logger.getLogger("Report").debug("Tokenizing tweets...")
    val tokenizer = new Tokenizer().
      setInputCol("_2").
      setOutputCol("tokens")

    val longString: Dataset[(Date, String)] = dataset.map( e => (e.date, e.topWords.reduce(_++ " " ++ _)))
    tokenizer.transform(longString).select("_1", "tokens")
  }

  def countVectorizer(df: DataFrame): DataFrame = {
    Logger.getLogger("Report").debug("CountVectorizing tweets...")
    val cvModel: CountVectorizerModel = new CountVectorizer().
      setInputCol("tokens").
      setOutputCol("features").
      setVocabSize(10).
      setMinDF(2).
      fit(df)

    cvModel.transform(df).select("_1", "features")
  }

  // Hyperparameters
  val k = 10
  val iter = 10

  /**
   * Latent dirichlet allocation
   * Multivariate beta distribution for topic clustering
   * in corpora
   */
  def lda(dataset: DataFrame): DataFrame = {
    Logger.getLogger("Report").debug("LDA Clustering...")
    val lda = new LDA().setK(k).setMaxIter(iter)
    val model = lda.fit(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)

    val topics = model.describeTopics(3)

    val transformed = model.transform(dataset).select("_1", "topicDistribution")
    transformed
  }


  def combine(df1: DataFrame, df2: DataFrame): DataFrame = {
    val indexer = new StringIndexer().
      setInputCol("confirmed").
      setOutputCol("label")

    val featureCols = Array("deaths", "mentions") //, "recovered", "deaths", "workplaces", "transit", "walking", "residential", "parks", "grocery", "driving", "stations", "retail", "waze")
    val assembler = new VectorAssembler().
      setInputCols(featureCols).
      setOutputCol("features")
    assembler.setHandleInvalid("skip")

    val featureDF = assembler.transform(df)
    val labelDF = indexer.fit(featureDF).transform(featureDF)
    labelDF
  }

  def randomForest(df: DataFrame): RandomForestClassificationModel =  {
    Logger.getLogger("Report").debug("Random foresting...")

    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

    val rf = new RandomForestClassifier().
      setNumTrees(10)

    val model = rf.fit(trainingData)
    val predictions = model.transform(testData)
    predictions show(5, false)


    val evaluator = new RegressionEvaluator().
      setLabelCol("label").
      setPredictionCol("prediction").
      setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    print("Model RMSE: ")
    println(rmse)

    model
  }




  def execute(masterDF: DataFrame, tweetsDS: Dataset[TwitterDay]): Unit = {
    val tokens = tokenizer(tweetsDS)
    val vectors = Util.time(countVectorizer(tokens))
    val topics = Util.time(lda(vectors))


//    val input = combineForRandomForest(masterDF, results)
    val rfInput: DataFrame = combine(masterDF, topics)
    val b = randomForest(a)


//      val assembled = dataFrameAssembler(data)
//      val matrix = correlationStatistic(assembled, spearman = false)
//      println(matrix)
  }
}

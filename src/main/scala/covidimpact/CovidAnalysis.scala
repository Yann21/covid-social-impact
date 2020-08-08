package covidimpact

import java.sql.Date

import org.apache.log4j.Logger
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

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
  val kTopics = 10
  val iter = 10

  /**
   * Latent dirichlet allocation
   * Multivariate beta distribution for topic clustering
   * in corpora
   */
  def lda(dataset: DataFrame): DataFrame = {
    Logger.getLogger("Report").debug("LDA Clustering...")
    val lda = new LDA().setK(kTopics).setMaxIter(iter)
    val model = lda.fit(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)

    val topics = model.describeTopics(4)
    println("Topic description")
    topics show (5, false)

    val transformed = model.transform(dataset).select("_1", "topicDistribution")
    transformed
  }


  def rfAssembler(df1: DataFrame, df2: DataFrame, includeJH: Boolean = true): DataFrame = {
    val topics = df2.withColumn("xs", vector_to_array($"topicDistribution")).
      select($"_1", $"xs"(0).as("lda_0"), $"xs"(1).as("lda_1"),
        $"xs"(2).as("lda_2"), $"xs"(3).as("lda_3"),
        $"xs"(4).as("lda_4"), $"xs"(5).as("lda_5"),
        $"xs"(6).as("lda_6"), $"xs"(7).as("lda_7"),
        $"xs"(8).as("lda_8"), $"xs"(9).as("lda_9"))

    val both: DataFrame = df1.join(topics, df1("date") === topics("_1"), "inner")

    val indexer = new StringIndexer().
      setInputCol("confirmed").
      setOutputCol("label")

    val featureCols =
      if (includeJH) Array("deaths", "mentions", "recovered", "deaths", "workplaces", "transit", "walking",
        "residential", "parks", "grocery", "driving", "stations", "retail", "waze",
        "lda_0", "lda_1", "lda_2", "lda_3", "lda_4", "lda_5", "lda_6", "lda_7", "lda_8", "lda_9")
      else Array("deaths", "mentions", "workplaces", "transit", "walking",
        "residential", "parks", "grocery", "driving", "stations", "retail", "waze",
        "lda_0", "lda_1", "lda_2", "lda_3", "lda_4", "lda_5", "lda_6", "lda_7", "lda_8", "lda_9")
    val assembler = new VectorAssembler().
      setInputCols(featureCols).
      setOutputCol("features")
    assembler.setHandleInvalid("skip")

    val featureDF = assembler.transform(both)
    val labeledDF = indexer.fit(featureDF).transform(featureDF)
    labeledDF
  }

  val rfTreeNb = 10
  def randomForest(df: DataFrame): RandomForestClassificationModel =  {
    Logger.getLogger("Report").debug("Random foresting...")

    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

    val rf = new RandomForestClassifier().
      setNumTrees(rfTreeNb)

    val model = rf.fit(trainingData)
    val predictions = model.transform(testData)
    println(model.toDebugString)

    val evaluator = new RegressionEvaluator().
      setLabelCol("label").
      setPredictionCol("prediction").
      setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    print("Model RMSE: "); println(rmse)

    model
  }




  def execute(masterDF: DataFrame, tweetsDS: Dataset[TwitterDay]): Unit = {
    val assembled = dataFrameAssembler(masterDF)
    val matrix = correlationStatistic(assembled, spearman = false)
    println(matrix)

    val tokens = tokenizer(tweetsDS)
    val vectors = Util.time(countVectorizer(tokens))
    val topics = Util.time(lda(vectors))


    val rfInputBiased: DataFrame = rfAssembler(masterDF, topics, includeJH = true)
    val rfInputUnbiased: DataFrame = rfAssembler(masterDF, topics, includeJH = false)

    val rfModelB  = Util.time( randomForest(rfInputBiased) )
    val rfModelUB = Util.time( randomForest(rfInputUnbiased) )
  }
}

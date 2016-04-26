package wongnai.mlservice.rest.controller

import org.apache.spark.ml.Model
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import wongnai.mlservice.Spark._
import wongnai.mlservice.api.searchranking.{ALSParamGrid, CrossValidationParams, NDCGParams, model}
import wongnai.mlservice.rest.PersonalizedSearchResult

import scala.util.{Success, Try}

/**
  * Created by ibosz on 24/3/59.
  */
object PersonalizationController {
  var recommendationModel: Model[_] = _

  var alsParamGrid: ALSParamGrid = ALSParamGrid(
    maxIter = Array(20),
    rank = Array(20),
    alpha = Array(0.01, 0.1, 0.5),
    regParam = Array(10.0, 100.0, 1000.0)
  )
  var ndcgParams = NDCGParams(recommendingThreshold = 10.0, k = 2 )
  var crossValidationParams = CrossValidationParams(numFolds = 3)

  var trainedModelResult = List[String]()

  def train(sourcePath: String): Unit = {
    val dataset = datasetFromCSV(sourcePath)
    recommendationModel =
      model.construct(dataset, alsParamGrid, ndcgParams, crossValidationParams)

    dataset.unpersist()
  }

  def trainWithoutEval(sourcePath: String): Unit = {
    val dataset = datasetFromCSV(sourcePath)

    recommendationModel = new ALS()
      .setImplicitPrefs(true)
      .setRank(100)
      .setMaxIter(20)
      .setAlpha(200.0)
      .setRegParam(0.1)
      .fit(dataset)
  }

  def rank(user: Int, items: List[Int]): PersonalizedSearchResult = {
    val model = recommendationModel.asInstanceOf[ALSModel]

    val rank = model.rank
    val userFeatures = model.userFactors.map {
      case Row(id: Int, factors: Seq[Float]) => (id, factors.toArray.map(_.toDouble))}

    val itemFactors = model.itemFactors.persist()

    val productFeatures = itemFactors
      .filter(col("id").isin(items: _*))
      .map {
        case Row(id: Int, factors: Seq[Float]) => (id, factors.toArray.map(_.toDouble))}

    val matrixFactorizationModel = new MatrixFactorizationModel(rank, userFeatures, productFeatures)
    val rankedItems = matrixFactorizationModel
      .recommendProducts(user, items.length)
      .map(_.product)
      .toList

    val notToRankItems = items diff {
      val knownItems = itemFactors.map { case Row(id: Int, _) => id }.collect
      val nonNegativePredictionItems = items
        .filter(item => {
          val tryPrediction = Try(matrixFactorizationModel.predict(user, item))
          tryPrediction match {
            case Success(prediction) => prediction > 0
            case _ => false
          }
        })

      knownItems ++ nonNegativePredictionItems
    }

    itemFactors.unpersist()

    PersonalizedSearchResult(user, rankedItems ++ notToRankItems)
  }

  def save(path: String): Unit = {
    recommendationModel.asInstanceOf[CrossValidatorModel].save(path)
  }

  def load(path: String): Unit = {
    recommendationModel = CrossValidatorModel.load(path)
  }

  private def datasetFromCSV(sourcePath: String) = {
    val getItem = udf { (entity: Int, entity_2: Int) => {
      val hasEntity2 = entity_2 > 0
      if(hasEntity2) entity_2 else entity
    }}

    val toDouble = udf[Double, Int]( _.toDouble)

    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(sourcePath)
      .filter(col("user").isNotNull and col("entity").isNotNull)
      .na.fill(-1d) // fill null in entity_2 with -1
      .select(col("user"), getItem(col("entity"), col("entity_2")).as("item"))
      .groupBy("user", "item")
      .count().withColumnRenamed("count", "rating")
      .withColumn("rating", toDouble(col("rating")))
      .persist()
  }
}

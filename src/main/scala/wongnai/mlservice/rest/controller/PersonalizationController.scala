package wongnai.mlservice.rest.controller

import org.apache.spark.ml.Model
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import wongnai.mlservice.Spark._
import wongnai.mlservice.api.searchranking.{model, ALSParamGrid, CrossValidationParams, NDCGParams}
import wongnai.mlservice.rest.PersonalizedSearchResult

/**
  * Created by ibosz on 24/3/59.
  */
object PersonalizationController {
  var recommendationModel: Model[_] = _

  var alsParamGrid: ALSParamGrid = ALSParamGrid(
    maxIter = Array(10),
    rank = Array(10),
    alpha = Array(10.0),
    regParam = Array(10.0)
  )
  var ndcgParams = NDCGParams(recommendingThreshold = 1.0, k = 2 )
  var crossValidationParams = CrossValidationParams(numFolds = 3)

  def train(sourcePath: String): Unit = {
    val getItem = udf { (entity: Int, entity_2: Int) => {
      val hasEntity2 = entity_2 > 0
      if(hasEntity2) entity_2 else entity
    }}

    val toDouble = udf[Double, Int]( _.toDouble)

    val dataset = sqlContext.read
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

    recommendationModel =
      model.construct(dataset, alsParamGrid, ndcgParams, crossValidationParams)

    dataset.unpersist()
  }

  def rank(user: Int, items: List[Int]): PersonalizedSearchResult = {
    val userItemsArray = items.map(item => (user, item))
    val userItemDF = sqlContext.createDataFrame(userItemsArray).toDF("user", "item")

    val StrangeBehaviourToZero = udf { prediction: Double =>
      if(prediction.isNaN || prediction < 0) 0D else prediction }

    val itemsDF = recommendationModel.transform(userItemDF)
      .withColumn("prediction", StrangeBehaviourToZero(col("prediction")))

    val zeroScoreItemsDF = itemsDF.filter(col("prediction") === 0D)
      .map { case Row(_, item: Int, _) => item }

    val sortedItemsDF = itemsDF
      .filter(col("prediction") !== 0D)
      .sort(desc("prediction"))
      .map { case Row(_, item: Int, _) => item }

    val rankedItems = (sortedItemsDF ++ zeroScoreItemsDF).collect.toList

    PersonalizedSearchResult(user, rankedItems)
  }

  def save(path: String): Unit = {
    recommendationModel.asInstanceOf[CrossValidatorModel].save(path)
  }

  def load(path: String): Unit = {
    recommendationModel = CrossValidatorModel.load(path)
  }
}

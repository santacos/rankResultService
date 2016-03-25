package wongnai.mlservice.rest.controller

import org.apache.spark.ml.Model
import org.apache.spark.sql.functions._
import wongnai.mlservice.Spark._
import wongnai.mlservice.api.searchranking.{model, ALSParamGrid, CrossValidationParams, NDCGParams}

/**
  * Created by ibosz on 24/3/59.
  */
object PersonalizationController {
  var recommendationModel: Model[_] = _

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


    val alsParamGrid = ALSParamGrid(
      maxIter = Array(1),
      rank = Array(1),
      alpha = Array(10.0),
      regParam = Array(10.0)
    )

    val ndcgParams = NDCGParams(
      recommendingThreshold = 10.0,
      k = 2
    )

    val crossValidationParams = CrossValidationParams(numFolds = 3)

    recommendationModel =
      model.construct(dataset, alsParamGrid, ndcgParams, crossValidationParams)

    dataset.unpersist()
  }
}

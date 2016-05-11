package wongnai.mlservice.api.searchranking

import org.apache.spark.sql.Row
import org.scalatest.{Matchers, FunSuite}
import testutil.FunSuiteSpark._
/**
  * Created by santacos on 4/26/2016 AD.
  */
class model$Test extends FunSuite with Matchers {

  ignore("Construct should create recommendation model correctly.") {
    sparkContext.setCheckpointDir("/Users/santacos/Desktop/")
    val dataset = sqlContext.createDataFrame(Seq(
      (1,2,4.0),
      (1,3,3.0),
      (1,4,5.0),
      (2,2,4.0),
      (2,3,3.0),
      (2,5,5.0),
      (3,11,4.0),
      (3,15,3.0),
      (3,9,5.0)
    )).toDF("user", "item", "rating")

    val alsParamGrid: ALSParamGrid = ALSParamGrid(
      maxIter = Array(20),
      rank = Array(20),
      alpha = Array(100.0),
      regParam = Array(1.0)
    )
    val ndcgParams = NDCGParams(recommendingThreshold = 1.0, k = 2 )
    val crossValidationParams = CrossValidationParams(numFolds = 3,samplingSize = 0.001)

    val CVmodel = model.construct(dataset,alsParamGrid,ndcgParams,crossValidationParams)

    val testSet = sqlContext.createDataFrame(Seq(
      (1, 15),
      (1, 11),
      (1, 9),
      (1, 2),
      (1, 3),
      (1, 4),
      (1, 5)
    )).toDF("user", "item")

    val result = CVmodel
      .transform(testSet)
      .map{ case Row(user, item, rating: Float) => (user, item, rating) }
      .collect.sortBy(rating => -rating._3)
      .map{ case (user, item, rating) => item }
      .take(4)

    result shouldBe Array(2, 3, 4, 5)
  }

}

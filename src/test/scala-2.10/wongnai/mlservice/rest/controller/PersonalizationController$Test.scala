package wongnai.mlservice.rest.controller

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import wongnai.mlservice.Spark._

/**
  * Created by santacos on 5/11/2016 AD.
  */
class PersonalizationController$Test extends FunSuite {

  val rank = 2
  val userFeatures: RDD[(Int, Array[Double])] = sparkContext.parallelize(List(
    1 -> Array(3, 4),
    2 -> Array(2, 1)
  ))
  val productFeatures: RDD[(Int, Array[Double])] = sparkContext.parallelize(List(
    1 -> Array(3, 4),
    2 -> Array(2, 1)
  ))

  //PersonalizationController.recommendationModel = (new MatrixFactorizationModel(rank, userFeatures, productFeatures)).asInstanceOf[]
  test("testRank") {

  }

}

package wongnai.mlservice.api.searchranking.recmodel

import org.apache.spark.rdd.RDD
import org.scalatest.Matchers
import testutil.FunSuiteSpark

/**
  * Created by ibosz on 8/3/59.
  */
class MatrixFactorizationRecommendationModelTest extends FunSuiteSpark with Matchers {
  import FunSuiteSpark.sparkContext

  val rank = 2
  val userFeatures: RDD[(Int, Array[Double])] = sparkContext.parallelize(List(
    1 -> Array(3, 4),
    2 -> Array(2, 1)
  ))
  val prodFeatures: RDD[(Int, Array[Double])] = sparkContext.parallelize(List(
    1 -> Array(3, 4),
    2 -> Array(2, 1)
  ))

  val matrixFactorizationRecommendationModel =
    new MatrixFactorizationRecommendationModel(rank, userFeatures, prodFeatures)

  test("check if user is available") {
    matrixFactorizationRecommendationModel.isUserAvailable(1) should be(true)
    matrixFactorizationRecommendationModel.isUserAvailable(3) should be(false)
  }
}

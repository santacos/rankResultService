package integrationtest

import org.apache.spark.rdd.RDD
import org.scalatest.Matchers
import service.ml.model.MatrixFactorizationRecommendationModel
import service.searchranking.{UserNotFoundException, SearchResultRanker}
import testutil.FunSuiteSpark

import scala.util.Try

/**
  * Created by ibosz on 8/3/59.
  */
class SearchResultRankingWithMatrixFactorizationTest extends FunSuiteSpark with Matchers {
  import FunSuiteSpark.sparkContext

  val rank = 2
  val userFeatures: RDD[(Int, Array[Double])] = sparkContext.parallelize(List(
    1 -> Array(1, 2)
  ))
  val prodFeatures: RDD[(Int, Array[Double])] = sparkContext.parallelize(List(
    1 -> Array(3, 4),
    2 -> Array(2, 1),
    3 -> Array(4, 2)
  ))

  val matrixFactorizationRecommendationModel =
    new MatrixFactorizationRecommendationModel(rank, userFeatures, prodFeatures)

  val ranker = new SearchResultRanker(matrixFactorizationRecommendationModel)

  test("ranker should return empty list if input is empty") {
    val user = 1
    val items: List[Int] = List()
    ranker.rank(user, items) should be(Try(List()))
  }

  test("ranker should rank correctly") {
    val user = 1
    val items: List[Int] = List(1, 2, 3)

    /*
    * score
    * 1, 1 => 11
    * 1, 2 => 4
    * 1, 3 => 8
    */

    ranker.rank(user, items) should be(Try(List(1, 3, 2)))
  }

  test("return UserNotFoundException when can not find user in model") {
    val user = 99

    ranker.rank(99, List()) should be {
      Try(throw new UserNotFoundException(s"user $user is not available in the model"))
    }
  }
}

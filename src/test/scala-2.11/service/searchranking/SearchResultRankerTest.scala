package service.searchranking

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, Matchers}
import service.ml.model.RecommendationModel

import scala.util.Try

/**
  * Created by ibosz on 7/3/59.
  */
class SearchResultRankerTest extends FunSuite with Matchers with MockFactory {
  val recommendationModelStub = stub[RecommendationModel]

  test("rank should return empty List") {
    recommendationModelStub.isUserAvailable _ when * returns true

    val ranker = new SearchResultRanker(recommendationModelStub)
    val user = 1
    val items: List[Int] = List()
    ranker.rank(user, items) should be (Try(List()))
  }

  test("rank should return list of ranked items for the user") {
    recommendationModelStub.predict _ when(1, 1) returns 1.0
    recommendationModelStub.predict _ when(1, 2) returns 2.1
    recommendationModelStub.predict _ when(1, 3) returns 3.2
    recommendationModelStub.predict _ when(1, 4) returns 5.0

    recommendationModelStub.isUserAvailable _ when * returns true

    val ranker = new SearchResultRanker(recommendationModelStub)
    val user = 1
    val items: List[Int] = List(1, 2, 3, 4)
    ranker.rank(user, items) should be (Try(List(4, 3, 2, 1)))
  }

  test("return UserNotFoundException when can not find user in model") {
    recommendationModelStub.isUserAvailable _ when 1 returns false

    val ranker = new SearchResultRanker(recommendationModelStub)
    val user = 1

    ranker.rank(1, List()) should be {
      Try(throw new UserNotFoundException(s"user $user is not available in the model"))
    }
  }

}

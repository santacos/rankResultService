import IdType.{ItemId, UserId}
import org.scalatest.FunSuite

/**
  * Created by santacos on 3/1/2016 AD.
  */
class SortTest extends FunSuite{

  def testRankResult(filteredItem: List[Int], ranks: List[Int], expected: List[Int]) = {
    val personalizer = new SearchResultPersonalizer
    assert(personalizer.rankResult(filteredItem,ranks)==expected)
  }

  test("Rank by duplicated items"){
    testRankResult(List(1,2,3),List(3,3,4,4,4,1,5),List(3,1,2))
  }

  test("Rank by different items with "){
    testRankResult(List(8,10,3,2),List(3,7),List(3,8,10,2))
  }

  test("Rank by empty filtered items"){
    testRankResult(List(),List(2),List())
  }

  test("Rank by empty ranks"){
    testRankResult(List(2),List(),List(2))
  }

  test("Execution time"){
    testRankResult(List(0 to 100:_*),List(0 to 10000:_*).reverse,List(0 to 100:_*).reverse)
  }

  test("return recommended items for user"){
    val personalizer = new SearchResultPersonalizer
    val userId = 2
    val count = 1000
    val expectedItem = 932
    assert(personalizer.recommendItems(userId,count).contains(expectedItem))
  }

  def testPersonalize(userId: UserId, filteredItems: List[ItemId], expectedItems: List[ItemId]) = {
    val personalizer = new SearchResultPersonalizer
    assert(personalizer.personalize(NonPersonalizedSearchResult(userId,filteredItems)).items == expectedItems)
  }

  test("Personalize function"){
    testPersonalize(30263,List(12454,19806,4577,1111,7174),List(1111,7174,12454,4577,19806).reverse)
  }

  test("Personalize function if filteredItem is unknown"){
    testPersonalize(30263,List(1111,2222,3333,4444,5555),List(1111,2222,3333,4444,5555))
  }
}



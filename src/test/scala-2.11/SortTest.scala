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

  test("")
}



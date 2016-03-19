package org.apache.spark.ml.evaluation.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ibosz on 19/3/59.
  */
class RecommendingAggregationFunctionTest extends FunSuite with Matchers {
  class TestBuffer(
      val array:ArrayBuffer[Any]
    ) extends MutableAggregationBuffer {

    def +=(element: Any): this.type = {
      array += element
      this
    }

    override def update(i: Int, value: Any): Unit = {
      array(i) = value
    }

    override def get(i: Int): Any = array(i)

    override def length: Int = array.length

    override def copy(): Row = Row()
  }

  test("initialization") {
    val recommendingAggregationFunction =
      new RecommendingAggregationFunction("item", "prediction", numRecommendation = 2)
    val buffer = new TestBuffer(ArrayBuffer(0))

    recommendingAggregationFunction.initialize(buffer)

    val recommendations = buffer(0)

    recommendations shouldBe Array[Row]()
  }

  test("update when recommendation count less than recommendation limit") {
    val recommendingAggregationFunction =
      new RecommendingAggregationFunction("item", "prediction", numRecommendation = 2)

    val buffer = new TestBuffer(ArrayBuffer(
      Array(Row(2, 10.0D))
    ))

    val recommendationCandidate = Row(3, 9.0D) // Row(item, prediction)

    recommendingAggregationFunction.update(buffer, recommendationCandidate)

    val recommendations = buffer(0).asInstanceOf[Array[Row]]

    recommendations should ( contain (recommendationCandidate) and contain (Row(2, 10.0D)) )
  }

  test("update when recommendation count greater than or equal recommendation limit with poorer recommendation") {
    val recommendingAggregationFunction =
      new RecommendingAggregationFunction("item", "prediction", numRecommendation = 2)

    val buffer = new TestBuffer(
      ArrayBuffer(Array(
        Row(2, 10D),
        Row(3, 20D)))
    )

    val poorRecommendationCandidate = Row(4, 9D) // Row(item, prediction)
    val goodRecommendationCandidate = Row(5, 199D) // Row(item, prediction)

    recommendingAggregationFunction.update(buffer, poorRecommendationCandidate)

    val unchangedRecommendations = buffer(0).asInstanceOf[Array[Row]]

    unchangedRecommendations should (
      contain (Row(2, 10D)) and
      contain (Row(3, 20D))
    )
  }

  test("update when recommendation count greater than or equal recommendation limit with better recommendation") {
    val recommendingAggregationFunction =
      new RecommendingAggregationFunction("item", "prediction", numRecommendation = 2)

    val buffer = new TestBuffer(
      ArrayBuffer(Array(
        Row(2, 10D),
        Row(3, 20D)))
    )

    val goodRecommendationCandidate = Row(5, 199D) // Row(item, prediction)

    recommendingAggregationFunction.update(buffer, goodRecommendationCandidate)

    val updatedRecommendations = buffer(0).asInstanceOf[Array[Row]]

    updatedRecommendations should (
      contain (goodRecommendationCandidate) and
      contain (Row(3, 20D))
    )
  }
}

package org.apache.spark.ml.evaluation.aggregation

import org.apache.spark.sql.Row
import org.scalatest.{FunSuite, Matchers}
import testutil.AggregationBuffer

import scala.collection._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by ibosz on 19/3/59.
  */
class RecommendingAggregationFunctionTest
  extends FunSuite with Matchers with AggregationBuffer {

  test("initialization") {
    val recommendingAggregationFunction =
      new RecommendingAggregationFunction("item", "prediction", numRecommendation = 2)
    val buffer = new TestBuffer(ArrayBuffer(0))

    recommendingAggregationFunction.initialize(buffer)

    val recommendations = buffer(0)

    recommendations shouldBe Array[Row]()
  }

  test("update when recommendation is empty") {
    val recommendingAggregationFunction =
      new RecommendingAggregationFunction("item", "prediction", numRecommendation = 2)

    val buffer = new TestBuffer(ArrayBuffer(
      mutable.WrappedArray.empty
    ))

    val recommendationCandidate = Row(3, 9.0D) // Row(item, prediction)

    recommendingAggregationFunction.update(buffer, recommendationCandidate)

    val recommendations = buffer(0).asInstanceOf[mutable.WrappedArray[Row]]

    recommendations should ( contain (recommendationCandidate) and contain (Row(3, 9.0D)) )
  }

  test("update when recommendation count less than recommendation limit") {
    val recommendingAggregationFunction =
      new RecommendingAggregationFunction("item", "prediction", numRecommendation = 2)

    val buffer = new TestBuffer(ArrayBuffer(
      Array(Row(2, 10.0D)).toSeq
    ))

    val recommendationCandidate = Row(3, 9.0D) // Row(item, prediction)

    recommendingAggregationFunction.update(buffer, recommendationCandidate)

    val recommendations = buffer(0).asInstanceOf[mutable.WrappedArray[Row]]

    recommendations should ( contain (recommendationCandidate) and contain (Row(2, 10.0D)) )
  }

  test("update when recommendation count greater than or equal recommendation limit with poorer recommendation") {
    val recommendingAggregationFunction =
      new RecommendingAggregationFunction("item", "prediction", numRecommendation = 2)

    val buffer = new TestBuffer(
      ArrayBuffer(Array(
        Row(2, 10D),
        Row(3, 20D)
      ).toSeq)
    )

    val poorRecommendationCandidate = Row(4, 9D) // Row(item, prediction)
    val goodRecommendationCandidate = Row(5, 199D) // Row(item, prediction)

    recommendingAggregationFunction.update(buffer, poorRecommendationCandidate)

    val unchangedRecommendations = buffer(0).asInstanceOf[mutable.WrappedArray[Row]]

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
        Row(3, 20D)
      ).toSeq)
    )

    val goodRecommendationCandidate = Row(5, 199D) // Row(item, prediction)

    recommendingAggregationFunction.update(buffer, goodRecommendationCandidate)

    val updatedRecommendations = buffer(0).asInstanceOf[mutable.WrappedArray[Row]]

    updatedRecommendations should (
      contain (goodRecommendationCandidate) and
      contain (Row(3, 20D))
    )
  }

  test("merge two partials aggregate") {
    val recommendingAggregationFunction =
      new RecommendingAggregationFunction("item", "prediction", numRecommendation = 2)

    val mainBuffer = new TestBuffer(
      ArrayBuffer(Array(
        Row(2, 10D),
        Row(3, 200D)
      ).toSeq)
    )

    val companionBuffer = new TestBuffer(
      ArrayBuffer(Array(
        Row(5, 100D),
        Row(7, 20D)
      ).toSeq)
    )

    recommendingAggregationFunction.merge(mainBuffer, companionBuffer)

    val updatedRecommendations = mainBuffer(0).asInstanceOf[mutable.WrappedArray[Row]]

    updatedRecommendations should (
      contain (Row(3, 200D)) and
      contain (Row(5, 100D))
    )
  }

  test("evaluate final result") {
    val recommendingAggregationFunction =
      new RecommendingAggregationFunction("item", "prediction", numRecommendation = 2)

    val buffer = Row(
      Array(Row(3, 200D), Row(4, 20D), Row(1, 10D)).toSeq
    )

    recommendingAggregationFunction.evaluate(buffer) should be (Array(3, 4, 1))
  }

}

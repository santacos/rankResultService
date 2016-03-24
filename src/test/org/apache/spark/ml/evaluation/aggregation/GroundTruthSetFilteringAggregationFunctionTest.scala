package org.apache.spark.ml.evaluation.aggregation

import org.apache.spark.sql.Row
import org.scalatest.{Matchers, FunSuite}
import testutil.AggregationBuffer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by ibosz on 20/3/59.
  */
class GroundTruthSetFilteringAggregationFunctionTest
  extends FunSuite with Matchers with AggregationBuffer{

  test("initialization") {
    val groundTruthSetFilteringAggregationFunction =
      new GroundTruthSetFilteringAggregationFunction("item", "label", 0D)

    val buffer = new TestBuffer(ArrayBuffer(0))
    groundTruthSetFilteringAggregationFunction.initialize(buffer)

    val groundTruth = buffer(0)
    groundTruth shouldBe Array[Row]()
  }

  test("update ground truth set when input score is above threshold") {
    val groundTruthSetFilteringAggregationFunction =
      new GroundTruthSetFilteringAggregationFunction("item", "label", 10.5)

    val buffer = new TestBuffer(ArrayBuffer(
      Array(Row(2, 15.0D)).toSeq
    ))
    val input = Row(5, 20.5)

    groundTruthSetFilteringAggregationFunction.update(buffer, input)

    val groundTruth = buffer(0).asInstanceOf[mutable.WrappedArray[Row]]

    groundTruth should (
      contain (Row(2, 15.0D)) and
      contain (Row(5, 20.5))
    )
  }

  test("update ground truth set when input score is equal threshold") {
    val groundTruthSetFilteringAggregationFunction =
      new GroundTruthSetFilteringAggregationFunction("item", "label", 20.5)

    val buffer = new TestBuffer(ArrayBuffer(
      Array(Row(2, 15.0D)).toSeq
    ))
    val input = Row(5, 20.5)

    groundTruthSetFilteringAggregationFunction.update(buffer, input)

    val groundTruth = buffer(0).asInstanceOf[mutable.WrappedArray[Row]]

    groundTruth should (
      contain (Row(2, 15.0D)) and
        contain (Row(5, 20.5))
      )
  }

  test("update nothing when input score is below threshold") {
    val groundTruthSetFilteringAggregationFunction =
      new GroundTruthSetFilteringAggregationFunction("item", "label", 10.5)

    val buffer = new TestBuffer(ArrayBuffer(
      mutable.WrappedArray.empty
    ))
    val input = Row(5, 2.5)

    groundTruthSetFilteringAggregationFunction.update(buffer, input)

    val groundTruth = buffer(0).asInstanceOf[mutable.WrappedArray[Row]]
    groundTruth shouldBe mutable.WrappedArray.empty
  }

  test("merge two partial results together bluntly") {
    val groundTruthSetFilteringAggregationFunction =
      new GroundTruthSetFilteringAggregationFunction("item", "label", 10D)

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

    groundTruthSetFilteringAggregationFunction.merge(mainBuffer, companionBuffer)

    val mergedBuffer = mainBuffer(0).asInstanceOf[mutable.WrappedArray[Row]]

    mergedBuffer should (
      contain (Row(2, 10D)) and
      contain (Row(3, 200D)) and
      contain (Row(5, 100D)) and
      contain (Row(7, 20D))
    )
  }

  test("evaluate final result") {
    val groundTruthSetFilteringAggregationFunction =
      new GroundTruthSetFilteringAggregationFunction("item", "label", 10D)

    val buffer = Row(
      Array(Row(3, 200D), Row(4, 20D), Row(1, 10D)).toSeq
    )

    groundTruthSetFilteringAggregationFunction.evaluate(buffer) should be (Array(3, 4, 1))
  }

}

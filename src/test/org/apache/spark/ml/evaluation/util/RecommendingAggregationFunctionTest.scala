package org.apache.spark.ml.evaluation.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ibosz on 19/3/59.
  */
class RecommendingAggregationFunctionTest extends FunSuite with Matchers {
  class TestBuffer extends MutableAggregationBuffer {
    val array = new ArrayBuffer[Any]()

    def +=(element: Any): Unit = {
      array += element
    }

    override def update(i: Int, value: Any): Unit = {
      array(i) = value
    }

    override def get(i: Int): Any = array(i)

    override def length: Int = array.length

    override def copy(): Row = Row()
  }

  test("initialization") {
    val buffer = new TestBuffer
    false shouldBe true
  }
}

package testutil

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ibosz on 20/3/59.
  */
trait AggregationBuffer {

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
}

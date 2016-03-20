package org.apache.spark.ml.evaluation.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
  * Created by ibosz on 20/3/59.
  */
class GroundTruthSetFilteringAggregationFunction(
    itemCol: String,
    labelCol: String,
    threshold: Double
  ) extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = new StructType()
    .add(itemCol, IntegerType)
    .add(labelCol, DoubleType)

  override def bufferSchema: StructType = StructType(Array(
    StructField("groundtruth", ArrayType(
      new StructType()
        .add("item", IntegerType)
        .add("label", DoubleType)
    ))
  ))

  override def deterministic: Boolean = true

  override def dataType: DataType = ArrayType(IntegerType)

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array[Row]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val groundTruth = buffer.getAs[mutable.WrappedArray[Row]](0)
    val isInputScoreHigherOrEqualThreshold = input match {
      case Row(item, score: Double) => score >= threshold
    }

    if(isInputScoreHigherOrEqualThreshold) {
      buffer(0) = groundTruth :+ input
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val groundTruthPart1 = buffer1(0).asInstanceOf[mutable.WrappedArray[Row]]
    val groundTruthPart2 = buffer2(0).asInstanceOf[mutable.WrappedArray[Row]]

    buffer1(0) = groundTruthPart1 ++ groundTruthPart2
  }

  override def evaluate(buffer: Row): Any =
    buffer.getAs[mutable.WrappedArray[Row]](0)
      .map { case Row(item, _) => item }
}

package org.apache.spark.ml.evaluation.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by ibosz on 18/3/59.
  */
class RecommendingAggregationFunction(
    itemCol: String,
    predictionCol: String,
    numRecommendation: Int
  ) extends UserDefinedAggregateFunction {

  case class ItemPrediction(item: Int, prediction: Double)

  override def inputSchema: StructType = new StructType()
    .add(itemCol, IntegerType)
    .add(predictionCol, DoubleType)

  override def bufferSchema: StructType = StructType(Array(
    StructField("recommendations", ArrayType(
      new StructType()
        .add("item", IntegerType)
        .add("prediction", DoubleType)
    )),
    StructField("counter", IntegerType)
  ))

  override def deterministic: Boolean = true

  override def dataType: DataType = ArrayType(IntegerType)

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array[Double]() // recommendations
    buffer(1) = 0D // counter
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val recommendationCounter = buffer(1).asInstanceOf[Double]
    val minRecommendation = buffer(0).asInstanceOf[Array[Double]]

    val notExceedNumRecommendation = recommendationCounter <= numRecommendation
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  override def evaluate(buffer: Row): Any = ???


}

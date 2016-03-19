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
    ))
  ))

  override def deterministic: Boolean = true

  override def dataType: DataType = ArrayType(IntegerType)

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array[Row]() // recommendations
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val currentRecommendations = buffer(0).asInstanceOf[Array[Row]]
    val currentRecommendationCount = currentRecommendations.length

    def worseRecommendationReducer(worstSoFar: (Row, Int), pointing: (Row, Int)) =
      (worstSoFar, pointing) match {
        case ((Row(_, worstSoFarPrediction: Double), _), (Row(_, pointingPrediction: Double), _)) =>
          if (pointingPrediction < worstSoFarPrediction)
            pointing
          else
            worstSoFar
      }

    val worstRecommendation: (Row, Int) = currentRecommendations
      .zipWithIndex
      .reduce(worseRecommendationReducer)

    val lessThanNumRecommendation = currentRecommendationCount < numRecommendation
    val foundBetterRecommendation = {
      val inputPrediction = input match { case Row(_, prediction: Double) => prediction }
      val currentWorstPrediction = worstRecommendation match {
        case (Row(_, prediction: Double), _) => prediction
      }

      inputPrediction > currentWorstPrediction
    }

    if(lessThanNumRecommendation) {
      buffer(0) = currentRecommendations :+ input
    } else if(foundBetterRecommendation) {
      val worstRecommendationIndex = worstRecommendation match {
        case (_, index) => index
      }
      buffer(0) = {
        currentRecommendations(worstRecommendationIndex) = input
        currentRecommendations
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  override def evaluate(buffer: Row): Any = ???


}

package org.apache.spark.ml.evaluation.aggregation

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

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
    val currentRecommendations = buffer.getAs[mutable.WrappedArray[Row]](0)
    val currentRecommendationCount = currentRecommendations.length

    val isCurrentCountLessThanNumRecommendation = currentRecommendationCount < numRecommendation

    if(currentRecommendations.isEmpty || isCurrentCountLessThanNumRecommendation) {
      buffer(0) = currentRecommendations :+ input
    } else {
      val worstRecommendationWithIndex = findWorstRecommendationWithIndex(currentRecommendations)
      val foundBetterRecommendation = isBetterRecommendation(worstRecommendationWithIndex, input)

     if(foundBetterRecommendation) {
        val worstRecommendationIndex = worstRecommendationWithIndex match {
          case (_, index) => index
        }
        buffer(0) = {
          currentRecommendations(worstRecommendationIndex) = input
          currentRecommendations
        }
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val recommendationsPart1 = buffer1(0).asInstanceOf[mutable.WrappedArray[Row]]
    val recommendationsPart2 = buffer2(0).asInstanceOf[mutable.WrappedArray[Row]]

    val combinedRecommendation = recommendationsPart1 ++ recommendationsPart2

    def recommendationPredictionDesc(recommendation: Row) = recommendation match {
      case Row(_, prediction: Double) => -prediction
    }

    buffer1(0) = combinedRecommendation
      .sortBy(recommendationPredictionDesc)
      .take(numRecommendation)
  }

  override def evaluate(buffer: Row): Any =
    buffer.getAs[mutable.WrappedArray[Row]](0)
      .map { case Row(item, _) => item }


  // helper functions

  private def findWorstRecommendationWithIndex(currentRecommendations: Seq[Row]): (Row, Int) = {
    def worseRecommendationReducer(worstSoFar: (Row, Int), pointing: (Row, Int)) =
      (worstSoFar, pointing) match {
        case ((Row(_, worstSoFarPrediction: Double), _), (Row(_, pointingPrediction: Double), _)) =>
          if (pointingPrediction < worstSoFarPrediction)
            pointing
          else
            worstSoFar
      }

    currentRecommendations
      .zipWithIndex
      .reduce(worseRecommendationReducer)
  }

  private def isBetterRecommendation(worstRecommendation: (Row, Int), input: Row) = {
    val inputPrediction = input match { case Row(_, prediction: Double) => prediction }
    val currentWorstPrediction = worstRecommendation match {
      case (Row(_, prediction: Double), _) => prediction
    }

    inputPrediction > currentWorstPrediction
  }
}

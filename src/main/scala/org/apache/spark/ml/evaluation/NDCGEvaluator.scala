package org.apache.spark.ml.evaluation

import org.apache.spark.Logging
import org.apache.spark.ml.Model
import org.apache.spark.ml.evaluation.util.{GroundTruthSetFilteringAggregationFunction, RecommendingAggregationFunction}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by ibosz on 14/3/59.
  */

private[evaluation] trait NDCGParams extends Params
  with HasInputCols with HasLabelCol with HasPredictionCol
  with Logging
{
  val k: Param[Int] =
    new Param(this, "k", "number of item that should be recommended to be used to evaluate")
  def getK: Int = $(k)

  val recommendingThreshold =
    new Param[Double](this, "recommendingThreshold",
      "threshold for determining if an item should be recommended for user or not")
  def getRecommendingThreshold: Double = $(recommendingThreshold)

  val userCol = new Param[String](this, "userCol", "column name for user ids")
  def getUserCol: String = $(userCol)

  val itemCol = new Param[String](this, "itemCol", "column name for item ids")
  def getItemCol: String = $(itemCol)

}


class NDCGEvaluator(override val uid: String)
  extends RankingMetricEvaluator
  with NDCGParams
{
  def this() = this(Identifiable.randomUID("ndcgEval"))

  def setK(value: Int): this.type = set(k, value)
  def setRecommendingThreshold(value: Double): this.type = set(recommendingThreshold, value)

  def setUserCol(value: String): this.type  = set(userCol, value)
  def setItemCol(value: String): this.type  = set(itemCol, value)
  def setLabelCol(value: String): this.type  = set(labelCol, value)

  setDefault(k -> 10)
  setDefault(recommendingThreshold -> 0D)

  setDefault(userCol -> "user")
  setDefault(itemCol -> "item")
  setDefault(labelCol -> "rating")

  override def evaluate(dataset: DataFrame): Double = {
    throw new Exception("evaluate function from 'NDCGEvaluator' should not be used")
    0.0D
  }

  override def evaluateWithModel(dataset: DataFrame, model: Model[_], allUserItems: DataFrame): Double = {
    val schema = dataset.schema

    val labelColName = $(labelCol)
    val labelType = schema($(labelCol)).dataType
    require(labelType == FloatType || labelType == DoubleType,
      s"Label column $labelColName must be of type float or double, but not $labelType")

    val predictedTable = model.transform(allUserItems)

    // ** aggregation functions **

    val recommendingAggregationFunction =
      new RecommendingAggregationFunction($(itemCol), $(predictionCol), numRecommendation = $(k))

    val groundTruthFilter =
      new GroundTruthSetFilteringAggregationFunction(
        $(itemCol), $(labelCol), $(recommendingThreshold))

    // ** aggregation functions ** END

    val recommendedTable = predictedTable
      .groupBy($(userCol))
      .agg(
        recommendingAggregationFunction(col($(itemCol)), col($(predictionCol)))
          .alias("recommended"))

    val groundTruthTable = dataset
      .groupBy($(userCol))
      .agg(
        groundTruthFilter(col($(itemCol)), col($(labelCol)))
          .alias("ground_truth"))


    val predictionAndLabels = recommendedTable.join(groundTruthTable, $(userCol))
      .select("recommended", "ground_truth")
      .map {
        case Row(
          recommended: Seq[Int],
          groundTruth: Seq[Int]
        ) => (recommended.toArray, groundTruth.toArray)}

    new RankingMetrics(predictionAndLabels).ndcgAt($(k))
  }

  override def copy(extra: ParamMap): Evaluator = defaultCopy(extra)

}

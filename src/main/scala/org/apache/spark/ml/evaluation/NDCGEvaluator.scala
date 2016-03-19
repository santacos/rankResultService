package org.apache.spark.ml.evaluation

import org.apache.spark.Logging
import org.apache.spark.ml.Model
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
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

    val users = allUserItems.select("user").rdd.map { case Row(user) => user }.distinct

    val predictedTable = model.transform(allUserItems)
    val shouldBeRecommendedTable = dataset
      .filter(col($(labelCol)) > $(recommendingThreshold))

    val predictionAndLabels = users.collect.map(user => {

      val predictions = predictedTable
        .filter(predictedTable($(userCol)) === user)
        .sort(desc($(predictionCol)))
        .limit($(k)) // k
        .select($(userCol), $(itemCol))

      val labels = shouldBeRecommendedTable
        .filter(col("user") === user)
        .select($(userCol), $(itemCol))

      (predictions, labels)
    })

//    new RankingMetrics(predictionAndLabels).ndcgAt($(k))
    0.0D

  }

  override def copy(extra: ParamMap): Evaluator = defaultCopy(extra)

}

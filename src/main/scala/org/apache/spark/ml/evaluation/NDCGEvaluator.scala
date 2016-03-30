package org.apache.spark.ml.evaluation

import org.apache.spark.Logging
import org.apache.spark.ml.Model
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, FloatType}
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by ibosz on 14/3/59.
  */

private[evaluation] trait NDCGParams extends Params
  with HasInputCols with HasLabelCol with HasPredictionCol
{
  val k =
    new Param[Int](this, "k", "number of item that should be recommended to be used to evaluate") {
      override def jsonEncode(value: Int): String =
        compact(render(JInt(value)))

      override def jsonDecode(json: String): Int =
        parse(json) match { case JInt(x) => x.asInstanceOf[BigInt].toInt }
    }
  def getK: Int = $(k)

  val recommendingThreshold =
    new Param[Double](this, "recommendingThreshold",
      "threshold for determining if an item should be recommended for user or not") {
      override def jsonEncode(value: Double): String =
        compact(render(JDouble(value)))

      override def jsonDecode(json: String): Double =
        parse(json) match { case JDouble(x) => x.asInstanceOf[Double] }
    }

  def getRecommendingThreshold: Double = $(recommendingThreshold)

  val userCol = new Param[String](this, "userCol", "column name for user ids")
  def getUserCol: String = $(userCol)

  val itemCol = new Param[String](this, "itemCol", "column name for item ids")
  def getItemCol: String = $(itemCol)

}


class NDCGEvaluator(override val uid: String)
  extends RankingMetricEvaluator
  with NDCGParams
  with DefaultParamsWritable
  with Logging
{
  def this() = this(Identifiable.randomUID("ndcgEval"))

  def setK(value: Int): this.type = set(k, value)
  def setRecommendingThreshold(value: Double): this.type = set(recommendingThreshold, value)

  def setUserCol(value: String): this.type  = set(userCol, value)
  def setItemCol(value: String): this.type  = set(itemCol, value)
  def setLabelCol(value: String): this.type  = set(labelCol, value)
  def setPredictionCol(value: String): this.type  = set(predictionCol, value)

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

    val recommended = predictedTable
        .select($(userCol), $(itemCol), $(predictionCol))
        .map{ case Row(user, item: Int, prediction: Double) => user -> (item, prediction) }
        .aggregateByKey(Array[(Int, Double)]())(
          (itemAndPredictions, itemAndPrediction) => itemAndPredictions :+ itemAndPrediction,
          (itemAndPredictions1, itemAndPredictions2) => itemAndPredictions1 ++ itemAndPredictions2 )
        .map {
          case (user, itemAndPredictions: Array[(Int, Double)]) => {
            val recommendedItems = itemAndPredictions
              .sortBy{ case (_, prediction) => prediction }(Ordering.Double.reverse)
              .map{ case (item, prediction) => item }

            (user, recommendedItems)
          }
        }

    val groundTruth = dataset
        .select($(userCol), $(itemCol), $(labelCol))
        .map{ case Row(user, item: Int, label: Double) => user -> (item, label) }
      .aggregateByKey(Array[Int]())(
        (itemAndLabels, itemAndLabel) =>
          if(itemAndLabel._2 >= $(recommendingThreshold))
            itemAndLabels :+ (itemAndLabel match { case (item, _) => item })
          else
            itemAndLabels,
        (itemAndLabels1, itemAndLabels2) => itemAndLabels1 ++ itemAndLabels2 )

    val predictionAndLabels = recommended.join(groundTruth)
      .map { case (_, predictionAndLabel) => predictionAndLabel }

    ndcgAt($(k), predictionAndLabels)
  }

  private def ndcgAt(
      k: Int, predictionAndLabels: RDD[(Array[Int], Array[Int])]): Double = {
    require(k > 0, "ranking position k should be positive")
    predictionAndLabels.map { case (pred: Array[Int], lab: Array[Int]) =>
      val labSet = lab.toSet

      if (labSet.nonEmpty) {
        val labSetSize = labSet.size
        val n = math.min(math.max(pred.length, labSetSize), k)
        var maxDcg = 0.0
        var dcg = 0.0
        var i = 0
        while (i < n) {
          val gain = 1.0 / math.log(i + 2)
          if (labSet.contains(pred(i))) {
            dcg += gain
          }
          if (i < labSetSize) {
            maxDcg += gain
          }
          i += 1
        }
        dcg / maxDcg
      } else {
        logWarning("Empty ground truth set, check input data")
        0.0
      }
    }.mean()
  }

  override def copy(extra: ParamMap): Evaluator = defaultCopy(extra)

}

object NDCGEvaluator extends DefaultParamsReadable[NDCGEvaluator] {
  override def load(path: String): NDCGEvaluator = super.load(path)
}
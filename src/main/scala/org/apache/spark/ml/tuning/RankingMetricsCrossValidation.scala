package org.apache.spark.ml.tuning

import com.github.fommil.netlib.F2jBLAS
import org.apache.spark.ml.Model
import org.apache.spark.ml.evaluation.{Evaluator, RankingMetricEvaluator}
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame

/**
  * Created by ibosz on 15/3/59.
  */

trait RankingMetricsCrossValidatorParams extends Params {
  val rankingEvaluator: Param[RankingMetricEvaluator] = new Param(this, "rankingMetricEvaluator",
    "rankingMetricEvaluator used to select hyper-parameters that maximize the validated ranking metric")

  def getRankingEvaluator: Evaluator = $(rankingEvaluator)
}

class RankingMetricsCrossValidator extends CrossValidator with RankingMetricsCrossValidatorParams {
  private val f2jBLAS = new F2jBLAS

  def setRankingEvaluator(value: RankingMetricEvaluator): this.type = set(rankingEvaluator, value)

  override def fit(dataset: DataFrame): CrossValidatorModel = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sqlCtx = dataset.sqlContext
    val est = $(estimator)
    val eval = $(rankingEvaluator)
    val epm = $(estimatorParamMaps)
    val numModels = epm.length
    val metrics = new Array[Double](epm.length)
    val splits = MLUtils.kFold(dataset.rdd, $(numFolds), 0)
    splits.zipWithIndex.foreach { case ((training, validation), splitIndex) =>
      val trainingDataset = sqlCtx.createDataFrame(training, schema).cache()
      val validationDataset = sqlCtx.createDataFrame(validation, schema).cache()
      // multi-model training
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")
      val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
      trainingDataset.unpersist()
      var i = 0
      while (i < numModels) {
        //TODO: remove hardcoded select. set params instead.
        val allUserItem = dataset.select("user").join(dataset.select("item")).distinct
        val metric = eval
          .evaluateWithModel(
            models(i).transform(validationDataset, epm(i)), models(i), allUserItem)

        logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
        metrics(i) += metric
        i += 1
      }
      validationDataset.unpersist()
    }
    f2jBLAS.dscal(numModels, 1.0 / $(numFolds), metrics, 1)
    logInfo(s"Average cross-validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
    logInfo(s"Best cross-validation metric: $bestMetric.")
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    copyValues(new CrossValidatorModel(uid, bestModel, metrics).setParent(this))
  }
}

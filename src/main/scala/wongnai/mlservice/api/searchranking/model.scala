package wongnai.mlservice.api.searchranking

import org.apache.spark.ml.Model
import org.apache.spark.ml.evaluation.NDCGEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{ParamGridBuilder, RankingMetricsCrossValidator}
import org.apache.spark.sql.DataFrame

/**
  * Created by ibosz on 24/3/59.
  */
case class ALSParamGrid(
  maxIter: Array[Int], rank: Array[Int], alpha: Array[Double], regParam: Array[Double])

case class NDCGParams(recommendingThreshold: Double, k: Int)
case class CrossValidationParams(numFolds: Int)

object model {
  def construct(
    dataset: DataFrame,
    alsParamGrid: ALSParamGrid,
    ndcgParams: NDCGParams,
    crossValidationParams: CrossValidationParams
  ): Model[_] = {
    val als = new ALS()
      .setImplicitPrefs(true)

    val paramGrid = new ParamGridBuilder()
      .addGrid(als.maxIter, alsParamGrid.maxIter)
      .addGrid(als.rank, alsParamGrid.rank)
      .addGrid(als.alpha, alsParamGrid.alpha)
      .addGrid(als.regParam, alsParamGrid.regParam)
      .build()

    val ndcg = new NDCGEvaluator()
      .setLabelCol(als.getRatingCol)
      .setPredictionCol(als.getPredictionCol)
      .setRecommendingThreshold(ndcgParams.recommendingThreshold)
      .setK(ndcgParams.k)

    val crossValidator = new RankingMetricsCrossValidator()
      .setEstimator(als)
      .setRankingEvaluator(ndcg)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(crossValidationParams.numFolds)

    crossValidator.fit(dataset)
  }
}
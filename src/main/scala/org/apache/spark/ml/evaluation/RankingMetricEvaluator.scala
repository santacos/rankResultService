package org.apache.spark.ml.evaluation

import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame

/**
  * Created by ibosz on 15/3/59.
  */
trait RankingMetricEvaluator extends Evaluator {
  def evaluateWithModel(dataset: DataFrame, model: Model[_]): Double
}

package org.apache.spark.ml.tuning

import org.apache.spark.ml.evaluation.NDCGEvaluator
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import testutil.FunSuiteSpark

/**
  * Created by ibosz on 15/3/59.
  */
class RankingMetricsCrossValidatorTest extends FunSuiteSpark with Matchers with MockFactory {
  val rankingEvalStub = new NDCGEvaluator
  test("setter should set rankingEvaluator correctly") {
    val rankingCV = new RankingMetricsCrossValidator()
      .setRankingEvaluator(rankingEvalStub)

//    rankingCV.getRankingEvaluator should be (rankingEvalStub)
  }
}

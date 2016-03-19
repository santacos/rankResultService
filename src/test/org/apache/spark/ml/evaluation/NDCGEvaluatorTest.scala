package org.apache.spark.ml.evaluation

import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.DataFrame
import org.scalactic.TolerantNumerics
import org.scalamock.proxy.ProxyMockFactory
import org.scalatest.Matchers
import testutil.FunSuiteSpark

/**
  * Created by ibosz on 14/3/59.
  */
class NDCGEvaluatorTest extends FunSuiteSpark with Matchers with ProxyMockFactory {
  import FunSuiteSpark.sqlContext

  test("k should be 10 by default") {
    val evaluator = new NDCGEvaluator
    evaluator.getK should be (10)
  }

  test("setK set k param correctly") {
    val evaluator = new NDCGEvaluator().setK(20)
    evaluator.getK should be (20)
  }

  test("userCol should be 'user' by default") {
    val evaluator = new NDCGEvaluator
    evaluator.getUserCol should be ("user")
  }

  test("set userCol name correctly") {
    val evaluator = new NDCGEvaluator().setUserCol("userID")
    evaluator.getUserCol should be ("userID")
  }

  test("itemCol should be 'item' by default") {
    val evaluator = new NDCGEvaluator
    evaluator.getItemCol should be ("item")
  }

  test("set itemCol name correctly") {
    val evaluator = new NDCGEvaluator().setItemCol("itemID")
    evaluator.getItemCol should be ("itemID")
  }

  test("labelCol should be 'rating' by default") {
    val evaluator = new NDCGEvaluator
    evaluator.getLabelCol should be ("rating")
  }

  test("set labelCol name correctly") {
    val evaluator = new NDCGEvaluator().setLabelCol("label")
    evaluator.getLabelCol should be ("label")
  }

  test("recommendingThreshold should be 0.0 by default") {
    val evaluator = new NDCGEvaluator
    evaluator.getRecommendingThreshold should be (0.0)
  }

  test("set recommendingThreshold correctly") {
    val evaluator = new NDCGEvaluator().setRecommendingThreshold(5.9)
    evaluator.getRecommendingThreshold should be (5.9)
  }

  test("evaluation throw exception when label is not real number") {
    val evaluator = new NDCGEvaluator

    val userFactors: DataFrame = sqlContext.createDataFrame(Seq(
      (1, Seq(1f, 2f)),
      (2, Seq(1f, 2f))
    )).toDF("id", "features")

    val itemFactors: DataFrame = sqlContext.createDataFrame(Seq(
      (1, Seq(1f, 2f)),
      (2, Seq(1f, 2f))
    )).toDF("id", "features")

    val model = new ALSModel("als", 2, userFactors, itemFactors)

    intercept[IllegalArgumentException] {
      val dataset = sqlContext.createDataFrame(Seq(
        (1, 1, "200D", 200.002),
        (1, 2, "200D", 200.001),
        (1, 3, "100D", 100.213)
      )).toDF("user", "item", "rating", "prediction")

      val allUserItems = sqlContext.createDataFrame(Seq((1,1))).toDF("user", "item")

      evaluator.evaluateWithModel(dataset, model, allUserItems)
    }
  }

  test("function evaluate should not be used") {
    val evaluator = new NDCGEvaluator
    val dataset = sqlContext.createDataFrame(Seq(
      (1, 1, 0D, 200.002),
      (1, 2, 200D, 200.001),
      (1, 3, 100D, 100.213)
    )).toDF("user", "item", "rating", "prediction")

    intercept[Exception] {
      evaluator.evaluate(dataset)
    }
  }

  test("evaluation evaluate correctly") {
    val userFactors: DataFrame = sqlContext.createDataFrame(Seq(
      (1, Seq(1f, 2f)),
      (2, Seq(1f, 2f))
    )).toDF("id", "features")

    val itemFactors: DataFrame = sqlContext.createDataFrame(Seq(
      (1, Seq(1f, 2f)),
      (2, Seq(1f, 2f))
    )).toDF("id", "features")

    class StubbedALSModel extends ALSModel("als", rank = 10, userFactors, itemFactors) {
      override def transform(dataset: DataFrame): DataFrame = sqlContext.createDataFrame(Seq(
        (1, 1, 100D),
        (1, 2, 99D),
        (1, 3, 1D),
        (2, 1, 1D),
        (2, 2, 100D),
        (2, 3, 98D)
      )).toDF("user", "item", "prediction")
    }

    val model = new StubbedALSModel

    val testSet = sqlContext.createDataFrame(Seq(
      (1, 1, 100D),
      (1, 2, 99D),
      (2, 1, 1D),
      (2, 3, 100D)
    )).toDF("user", "item", "rating")

    val allUserItems = sqlContext.createDataFrame(Seq(
      (1, 1),
      (1, 2),
      (1, 3),
      (2, 1),
      (2, 2),
      (2, 3)
    )).toDF("user", "item")

    val users = allUserItems.select("user").distinct

    val predictedTable = model.transform(allUserItems)
    val shouldBeRecommendedTable = testSet
      .filter(testSet("rating") > 1.0).rdd



    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.001)

    val evaluator = new NDCGEvaluator()
      .setK(3)
      .setRecommendingThreshold(1D)
      .setRecommendingThreshold(1.0)

//    evaluator.evaluateWithModel(testSet, model, allUserItems)



//    new RankingMetrics(predictionAndLabels).ndcgAt(2) should equal (0.8154648767857287)

  }
}

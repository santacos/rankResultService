package org.apache.spark.ml.evaluation

import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.DataFrame
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import testutil.FunSuiteSpark


/**
  * Created by ibosz on 14/3/59.
  */
class NDCGEvaluatorTest extends FunSuiteSpark with Matchers with MockFactory {
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

  test("recommendingThreshold should be 1.0 by default") {
    val evaluator = new NDCGEvaluator
    evaluator.getRecommendingThreshold should be (1.0)
  }

  test("set recommendingThreshold correctly") {
    val evaluator = new NDCGEvaluator().setRecommendingThreshold(5.9)
    evaluator.getRecommendingThreshold should be (5.9)
  }

  test("evaluation throw exception when prediction is not real number") {
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
        (1, 1, 200D, "200.002"),
        (1, 2, 200D, "200.001"),
        (1, 3, 100D, "100.213")
      )).toDF("user", "item", "rating", "prediction")

      evaluator.evaluateWithModel(dataset, model)
    }
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

      evaluator.evaluateWithModel(dataset, model)
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

  ignore("evaluation evaluate correctly") {
    val evaluator = new NDCGEvaluator

    val dataset = sqlContext.createDataFrame(Seq(
      (1, 1, 0D, 200.002),
      (1, 2, 200D, 200.001),
      (1, 3, 100D, 100.213)
    )).toDF("user", "item", "rating", "prediction")

    evaluator.evaluate(dataset)

  }

}

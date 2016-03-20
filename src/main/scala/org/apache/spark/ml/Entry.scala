package org.apache.spark.ml

import org.apache.spark.ml.evaluation.util.{GroundTruthSetFilteringAggregationFunction, RecommendingAggregationFunction}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ibosz on 4/3/59.
  */
object Entry extends App {
  lazy val sparkConfig = new SparkConf()
    .setAppName("test")
    .setMaster("local[*]")

  lazy val sparkContext = new SparkContext(sparkConfig)
  lazy val sqlContext = new SQLContext(sparkContext)

  val userFactors: DataFrame = sqlContext.createDataFrame(Seq(
    (1, Seq(1f, 2f)),
    (2, Seq(1f, 2f))
  )).toDF("id", "features")

  val itemFactors: DataFrame = sqlContext.createDataFrame(Seq(
    (1, Seq(1f, 2f)),
    (2, Seq(1f, 2f))
  )).toDF("id", "features")

  class StubbedALSModel extends ALSModel("als", 10, userFactors, itemFactors) {
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

  val groundTruthFilter =
    new GroundTruthSetFilteringAggregationFunction("item", "rating", 10D)

  val groundTruthTable = testSet
      .groupBy("user")
      .agg(
        groundTruthFilter(col("item"), col("rating"))
          .alias("ground_truth"))

  val recommendingAggregationFunction =
    new RecommendingAggregationFunction("item", "prediction", numRecommendation = 2)

  val recommendedTable = predictedTable
    .groupBy("user")
    .agg(
      recommendingAggregationFunction(col("item"), col("prediction"))
      .alias("recommended"))

  recommendedTable.join(groundTruthTable, "user")
    .select("recommended", "ground_truth")
    .map{ case Row(recommended, groundTruth) => (recommended, groundTruth)}
    .collect
    .foreach(println)
}

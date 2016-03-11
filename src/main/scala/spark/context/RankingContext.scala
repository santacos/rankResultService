package spark.context

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ibosz on 29/2/59.
  */
object RankingContext {
  val config = new SparkConf()
    .setAppName("rec")

  val sparkContext = new SparkContext(config)

  val modelDir = "/Users/santacos/senior_file/Explicit/model05"
}

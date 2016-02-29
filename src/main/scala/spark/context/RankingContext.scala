package spark.context

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ibosz on 29/2/59.
  */
object RankingContext {
  private val config = new SparkConf()
    .setMaster("local[4]")
    .setAppName("Sigmoid Spark")
    .set("spark.executor.memory", "2g")
    .set("spark.rdd.compress", "true")
    .set("spark.storage.memoryFraction", "0.5")

  lazy val sparkContext = new SparkContext(config)

  val modelDir = "/Users/santacos/senior_file/Explicit/model05"
}

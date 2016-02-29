package spark.context

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ibosz on 29/2/59.
  */
object RankingContext {
  private val config = ???
  lazy val sparkContext = new SparkContext(config)
}

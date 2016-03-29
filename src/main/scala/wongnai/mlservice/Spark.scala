package wongnai.mlservice

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import wongnai.mlservice.rest.ServerBootstrap

/**
  * Created by ibosz on 24/3/59.
  */
object Spark extends App {
  val sparkConf: SparkConf = new SparkConf()
    .setAppName("SearchResultRanker")

  var sparkContext: SparkContext = new SparkContext(sparkConf)
  var sqlContext: SQLContext = new SQLContext(sparkContext)

  ServerBootstrap.startServer()
}

package wongnai.mlservice

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ibosz on 24/3/59.
  */
object Spark {
  val sparkConf: SparkConf = new SparkConf()
    .setAppName("rank")
    .setMaster("local[*]")

  var sparkContext: SparkContext = new SparkContext(sparkConf)
  var sqlContext: SQLContext = new SQLContext(sparkContext)
}

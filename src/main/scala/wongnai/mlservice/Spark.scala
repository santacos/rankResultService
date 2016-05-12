package wongnai.mlservice

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import wongnai.mlservice.rest.ServerBootstrap

/**
  * Created by ibosz on 24/3/59.
  */
object Spark extends App {
  val config = ConfigFactory.load()
  val sparkConf: SparkConf = new SparkConf()
    .setAppName("SearchResultRanker")
    .set("spark.cassandra.connection.host", config.getString("cassandra.connection.host"))
    .set("spark.cassandra.auth.username"  , config.getString("cassandra.username"))
    .set("spark.cassandra.auth.password"  , config.getString("cassandra.password"))


  val sparkContext: SparkContext = new SparkContext(sparkConf)
  val sqlContext: SQLContext = new SQLContext(sparkContext)

  ServerBootstrap.startServer()
}

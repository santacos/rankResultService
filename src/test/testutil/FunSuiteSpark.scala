package testutil

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite

/**
  * Created by ibosz on 8/3/59.
  */
class FunSuiteSpark extends FunSuite

object FunSuiteSpark {
  lazy val sparkConfig = new SparkConf()
    .setAppName("test")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cassandra.connection.port", "9042")

  lazy val sparkContext = new SparkContext(sparkConfig)
  lazy val sqlContext = new SQLContext(sparkContext)
}
package testutil

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite

/**
  * Created by ibosz on 8/3/59.
  */
class FunSuiteSpark extends FunSuite

object FunSuiteSpark {
  private lazy val sparkConfig = new SparkConf()
    .setAppName("test")
    .setMaster("local[*]")

  lazy val sparkContext = new SparkContext(sparkConfig)
  lazy val sqlContext = new SQLContext(sparkContext)
}
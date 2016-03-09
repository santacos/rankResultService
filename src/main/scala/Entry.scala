import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ibosz on 4/3/59.
  */
object Entry extends App {
  val sparkConfig = new SparkConf()
    .setAppName("test")
    .setMaster("yarn-client")

  val sparkContext = new SparkContext(sparkConfig)

  val numbersRDD = sparkContext.parallelize(List(1, 2, 3, 4, 5))

  println {
    s"result is ${numbersRDD.reduce(_ + _)}"
  }
}

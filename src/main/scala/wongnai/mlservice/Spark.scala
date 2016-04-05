package wongnai.mlservice

import org.apache.spark.ml.evaluation.NDCGEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.RankingMetricsCrossValidator
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import wongnai.mlservice.rest.ServerBootstrap

/**
  * Created by ibosz on 24/3/59.
  */
object Spark extends App {

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("SearchResultRanker")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  sparkConf.registerKryoClasses(Array(
    classOf[NDCGEvaluator],
    classOf[RankingMetricsCrossValidator],
    classOf[Row],
    classOf[ALS],
    classOf[ALSModel]
  ))

  val sparkContext: SparkContext = new SparkContext(sparkConf)
  val sqlContext: SQLContext = new SQLContext(sparkContext)

  ServerBootstrap.startServer()
}

package wongnai.mlservice.io

import com.datastax.spark.connector._
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by ibosz on 17/5/59.
  */
class CassandraModelExporter(sc: SparkContext, keyspace: String, table: String) {
  def exportModel(
    rank: Int,
    userFactors: RDD[(Int, Array[Double])],
    itemFactors: RDD[(Int, Array[Double])]): Unit = {

    type Factor = (Int, Array[Double])

    (userFactors cartesian itemFactors)
      .map {
        case (user: Factor, item: Factor) =>
          (user._1, item._1, blas.ddot(rank, user._2, 1, item._2, 1)) }
      .saveToCassandra(keyspace, table, SomeColumns("user_id", "item_id", "score"))
  }
}

package wongnai.mlservice.io

import com.datastax.spark.connector._
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by ibosz on 17/5/59.
  */
class CassandraModelExporter(sc: SparkContext, keyspace: String, userTable: String, itemTable: String) {
  def exportModel(
    rank: Int,
    userFactors: RDD[(Int, Array[Double])],
    itemFactors: RDD[(Int, Array[Double])]): Unit = {

    val featureToVec: PartialFunction[(Int, Array[Double]), (Int, Vector[Double])] = {
      case (id: Int, features: Array[Double]) =>
        (id, features.toVector) }

    userFactors
      .map(featureToVec)
      .saveToCassandra(keyspace, userTable, SomeColumns("user_id", "features"))

    itemFactors
      .map(featureToVec)
      .saveToCassandra(keyspace, itemTable, SomeColumns("item_id", "features"))
  }






}

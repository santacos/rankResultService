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
    userFactors: RDD[(Int, Array[Double])],
    itemFactors: RDD[(Int, Array[Double])]): Unit = {

    saveFeatures(userFactors, itemFactors)

    val selectedUserFactors = userFactors.filter(_._1 <= 10000)
    savePredictions(itemFactors, selectedUserFactors)
  }

  private def savePredictions(
    itemFactors: RDD[(Int, Array[Double])],
    selectedUserFactors: RDD[(Int, Array[Double])]
  ): Unit = {
    type Factor = (Int, Array[Double])

    (selectedUserFactors cartesian itemFactors)
      .map {
        case (user: Factor, item: Factor) =>
          (user._1, item._1, blas.ddot(user._2.length, user._2, 1, item._2, 1))
      }
      .saveToCassandra(keyspace, "recommendation", SomeColumns("user_id", "item_id", "score"))
  }

  private def saveFeatures(userFactors: RDD[(Int, Array[Double])], itemFactors: RDD[(Int, Array[Double])]): Unit = {

    val featureToVec: PartialFunction[(Int, Array[Double]), (Int, Vector[Double])] = {
      case (id: Int, features: Array[Double]) => (id, features.toVector) }

    userFactors
      .map(featureToVec)
      .saveToCassandra(keyspace, userTable, SomeColumns("user_id", "features"))

    itemFactors
      .map(featureToVec)
      .saveToCassandra(keyspace, itemTable, SomeColumns("item_id", "features"))
  }
}

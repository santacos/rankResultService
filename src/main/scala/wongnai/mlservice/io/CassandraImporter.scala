package wongnai.mlservice.io

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

import scala.util.{Failure, Success, Try}

/**
  * Created by ibosz on 12/5/59.
  */
class CassandraImporter(sc: SparkContext, keyspace: String, table: String) {
  type Rating = (Int, Int, Double)

  def importData(): RDD[Rating] = {
    sc.cassandraTable(keyspace, table)
      .filter(isNotNull("user"))
      .filter(isNotNull("entity"))
      .map(toUserItem)
      .reduceByKey(_ + _)
      .map{ case ((user, item), rating) => (user, item, rating.toDouble)}
  }

  private val isNotNull = (colName: String) => (row: CassandraRow) =>
    row.getStringOption(colName) match {
      case Some(s) => if (s.trim == "") false else true
      case None    => false
    }

  private val toUserItem = (row: CassandraRow) => {
    val user = row.get[Int]("user")

    val nullableEntity = Try(row.get[Option[Int]]("entity_2")) match {
      case Success(optionInt) => optionInt
      case Failure(_) => None
    }

    val item = nullableEntity.getOrElse(row.get[Int]("entity"))

    (user, item) -> 1
  }
}

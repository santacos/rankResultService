package wongnai.mlservice.io

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

/**
  * Created by ibosz on 12/5/59.
  */
class CassandraImporter(sc: SparkContext, keyspace: String, table: String) extends Importer {
  type Rating = (Int, Int, Double)

  override def importData(): RDD[Rating] = {
    sc.cassandraTable(keyspace, table)
      .filter(isNotNull("user"))
      .filter(isNotNull("entity"))
      .map(toUserItem)
      .reduceByKey(_ + _)
      .map{ case ((user, item), rating) => (user, item, rating.toDouble)}
  }

  private val isNotNull = (colName: String) => (row: CassandraRow) =>
    row.getIntOption(colName) match {
      case Some(_) => true
      case None    => false
    }

  private val toUserItem = (row: CassandraRow) => {
    val user = row.get[Int]("user")

    val nullableEntity = row.get[Option[Int]]("entity_2")
    val item = nullableEntity.getOrElse(row.get[Int]("entity"))

    (user, item) -> 1
  }
}

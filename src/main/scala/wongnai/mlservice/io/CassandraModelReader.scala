package wongnai.mlservice.io

import com.datastax.spark.connector._
import org.apache.spark.SparkContext

/**
  * Created by ibosz on 17/5/59.
  */
class CassandraModelReader(sc: SparkContext, keyspace: String, table: String) {
  type Rating = (Int, Int, Double)

  def getPredictions(userId: Int, itemIds: List[Int]): List[Rating] = {
    val predictionTable = sc.cassandraTable[Rating](keyspace, table)
    val itemIdsString = s"(${itemIds mkString ","})"

    val queryResult = predictionTable
      .where(s"user_id = $userId")
      .where(s"item_id in $itemIdsString")
      .collect.toList

    val presentItems = queryResult
      .map { case (user_id, item_id, score) => item_id }

    val absentItems = itemIds diff presentItems

    val absentRatings = absentItems
      .map(itemId => (userId, itemId, 0.0))

    queryResult ++ absentRatings
  }
}

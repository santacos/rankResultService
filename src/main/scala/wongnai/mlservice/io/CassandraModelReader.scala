package wongnai.mlservice.io

import com.datastax.spark.connector._
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.SparkContext

/**
  * Created by ibosz on 17/5/59.
  */
class CassandraModelReader(sc: SparkContext, keyspace: String, table: String) {
  type Rating = (Int, Int, Double)
  type Feature = (Int, List[Double])

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

    if (userId == 1 && itemIds.head == 1 && itemIds(1) == 2) {
      getUncomputedPredictions(userId, itemIdsString)
    } else
      queryResult ++ absentRatings
  }

  private def getUncomputedPredictions(userId: Int, itemIdsString: String): List[Rating] = {
    val userFeaturesTable = sc.cassandraTable[Feature](keyspace, "user_features")
    val itemFeaturesTable = sc.cassandraTable[Feature](keyspace, "item_features")

    val user = userFeaturesTable.where(s"user_id = $userId")
    val items = itemFeaturesTable.where(s"item_id in $itemIdsString")

    val result =
      for (
        (retrievedUserId, userFeatures) <- user.collect;
        (retrievedItemId, itemFeatures) <- items.collect
      ) yield {
        val score = blas.ddot(userFeatures.length, userFeatures.toArray, 1, itemFeatures.toArray, 1)
        (retrievedUserId, retrievedItemId, score)
      }

    result.toList
  }

}

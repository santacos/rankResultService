package wongnai.mlservice.io

import com.datastax.spark.connector._
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.SparkContext

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try, Success}

/**
  * Created by ibosz on 17/5/59.
  */
class CassandraModelReader(sc: SparkContext, keyspace: String, table: String) {
  type Rating = (Int, Int, Double)
  type Feature = (Int, List[Double])

  def getPredictions(userId: Int, itemIds: List[Int]): List[Rating] = {
    val computedPredictions = getComputedPredictions(userId, itemIds)

    val computedItems = computedPredictions
      .map { case (_, itemId, _) => itemId }
    val uncomputedItems: List[Int] = itemIds diff computedItems

    val uncomputedPredictions = getUncomputedPredictions(userId, uncomputedItems)
    savePrediction(uncomputedPredictions)

    val existingResult = computedPredictions ++ uncomputedPredictions
    val existingItems = existingResult
      .map { case (user_id, item_id, score) => item_id }

    val nonExistingItems = itemIds diff existingItems
    val nonExistingResult = nonExistingItems
      .map(itemId => (userId, itemId, 0.0))

    existingResult ++ nonExistingResult
  }


  def savePrediction(predictions: List[Rating]): Future[Unit] = {
    import ExecutionContext.Implicits.global
    Future {
      sc.parallelize(predictions)
        .saveToCassandra(keyspace, table, SomeColumns("user_id", "item_id", "score"))
    }
  }

  private def getComputedPredictions(userId: Int, itemIds: List[Int]): List[Rating] = {
    val predictionTable = sc.cassandraTable[Rating](keyspace, table)

    predictionTable
      .where(s"user_id = $userId")
      .where(s"item_id in (${itemIds mkString ","})")
      .collect.toList
  }

  private def getUncomputedPredictions(userId: Int, itemIds: List[Int]): List[Rating] = {
    val userFeaturesTable = sc.cassandraTable[Feature](keyspace, "user_features")
    val itemFeaturesTable = sc.cassandraTable[Feature](keyspace, "item_features")

    val (_, userFeatures) = userFeaturesTable.where(s"user_id = $userId").collect.headOption match {
      case Some(idAndFeatures) => idAndFeatures
      case None => return List.empty
    }

    val items: List[(Int, List[Double])] =
      for {
        itemId <- itemIds
        idAndFeaturesList = itemFeaturesTable.where(s"item_id = $itemId").collect
        idAndFeatures <- idAndFeaturesList.headOption
      } yield idAndFeatures

    val result = items
      .map { case (itemId, itemFeatures) =>
        val score = blas.ddot(userFeatures.length, userFeatures.toArray, 1, itemFeatures.toArray, 1)
        (userId, itemId, score)
      }

    result
  }

}

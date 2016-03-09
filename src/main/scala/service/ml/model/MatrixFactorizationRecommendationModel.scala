package service.ml.model

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD

/**
  * Created by ibosz on 8/3/59.
  */
class MatrixFactorizationRecommendationModel(
    override val rank: Int,
    override val userFeatures: RDD[(Int, Array[Double])],
    override val productFeatures: RDD[(Int, Array[Double])])
  extends MatrixFactorizationModel(rank, userFeatures, productFeatures)
  with RecommendationModel {

  override def isUserAvailable(targetUser: Int) =
    userFeatures.lookup(targetUser).nonEmpty
}
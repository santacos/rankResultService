package wongnai.mlservice.api.searchranking.recmodel

/**
  * Created by ibosz on 7/3/59.
  */
trait RecommendationModel {
  def predict(user: Int, product: Int): Double
  def isUserAvailable(user: Int): Boolean
}
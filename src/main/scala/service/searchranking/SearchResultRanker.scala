package service.searchranking

import service.searchranking.recmodel.RecommendationModel

import scala.util.Try

/**
  * Created by ibosz on 7/3/59.
  */
class SearchResultRanker(recommendationModel: RecommendationModel) {

  def rank(user: Int, items: List[Int]): Try[List[Int]] = Try {
    if(!recommendationModel.isUserAvailable(user))
      throw new UserNotFoundException(s"user $user is not available in the model")
    else
      sortItemsForUser(user, items)
  }

  private def sortItemsForUser(user: Int, items: List[Int]): List[Int] =
    sortItemsByScore(predictScore(user, items))

  private def sortItemsByScore(itemsWithScore :List[(Int, Double)]): List[Int] =
    itemsWithScore
      .sortWith(_._2 > _._2)
      .map { case (item, score) => item }

  private def predictScore(user: Int, items: List[Int]): List[(Int, Double)] =
    items.map(item => {
      val score = recommendationModel.predict(user, item)
      (item, score)
    })
}

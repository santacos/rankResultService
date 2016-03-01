import IdType.{UserId, ItemId}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import spark.context.RankingContext._



class SearchResultPersonalizer {
  def recommendItems(userId: UserId, count: Int):List[ItemId] = {
    val model = MatrixFactorizationModel.load(sparkContext,modelDir)
    model.recommendProducts(userId,count).map(_.product).toList
  }

  def rankResult(l1: List[ItemId], l2: List[ItemId]):List[ItemId] = {
    val ranks = l2.distinct.filter(l1.contains(_))
    val nonranks = l1.filter(!l2.contains(_))
    ranks ::: nonranks
  }

  def personalize(nonPersonalizedSearchResult: NonPersonalizedSearchResult): PersonalizedSearchResult = {
    val ranks = rankResult(nonPersonalizedSearchResult.items,recommendItems(nonPersonalizedSearchResult.user,1000))
    PersonalizedSearchResult(ranks)
  }
}



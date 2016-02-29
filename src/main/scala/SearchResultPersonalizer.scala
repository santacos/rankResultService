import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import spark.context.RankingContext._
class SearchResultPersonalizer {

  def personalize(nonPersonalizedSearchResult: NonPersonalizedSearchResult): PersonalizedSearchResult = {
    //PersonalizedSearchResult(nonPersonalizedSearchResult.items)

    val explicitModel = MatrixFactorizationModel.load(sparkContext,modelDir)
    val recommendProduct = explicitModel.recommendProducts(nonPersonalizedSearchResult.user,10)
    PersonalizedSearchResult(recommendProduct.map(_.product).toList)


  }
}

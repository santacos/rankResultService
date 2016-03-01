import IdType.ItemId
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import spark.context.RankingContext._

import scala.util.Sorting

case class ScoredProduct(itemId: ItemId,rating:Double)

object ScoredProductOrdering extends Ordering[ScoredProduct]{
  def compare(a:ScoredProduct,b:ScoredProduct)=a.rating compare(b.rating)
}


class SearchResultPersonalizer {
  def rankResult(l1: List[ItemId], l2: List[ItemId]) = {
    val ranks = l2.distinct.filter(l1.contains(_))
    val nonranks = l1.filter(!l2.contains(_))
    ranks ::: nonranks
  }


  def personalize(nonPersonalizedSearchResult: NonPersonalizedSearchResult): PersonalizedSearchResult = {


    val explicitModel = MatrixFactorizationModel.load(sparkContext,modelDir)

    val user = nonPersonalizedSearchResult.user

    val scoredProducts = nonPersonalizedSearchResult.items.map(itemId =>
      {
        val rating = explicitModel.predict(user,itemId)
        ScoredProduct(itemId,rating)
      }).toArray

    Sorting.quickSort(scoredProducts)(ScoredProductOrdering)

    PersonalizedSearchResult(scoredProducts.map(_.itemId).toList)

  }



}



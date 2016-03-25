package wongnai.mlservice.rest

import akka.http.scaladsl.marshallers.sprayjson._
import spray.json.DefaultJsonProtocol

/**
  * Created by ibosz on 24/3/59.
  */

final case class SearchResult(user: Int, items: List[Int])
final case class PersonalizedSearchResult(user: Int, ranked_items: List[Int])

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val searchResultFormat = jsonFormat2(SearchResult)
  implicit val personalizedSearchResultFormat = jsonFormat2(PersonalizedSearchResult)
}

package wongnai.mlservice.rest

import akka.http.scaladsl.marshallers.sprayjson._
import spray.json.DefaultJsonProtocol
import wongnai.mlservice.api.searchranking.{NDCGParams, CrossValidationParams, ALSParamGrid}

/**
  * Created by ibosz on 24/3/59.
  */

final case class SearchResult(user: Int, items: List[Int])
final case class PersonalizedSearchResult(user: Int, ranked_items: List[Int])
final case class FileLocation(path: String)
final case class Message(error: Option[Boolean], message: Option[String])

final case class Params(
  alsParamGrid: ALSParamGrid, crossValidationParams: CrossValidationParams, ndcgParams: NDCGParams)

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val searchResultFormat = jsonFormat2(SearchResult)
  implicit val personalizedSearchResultFormat = jsonFormat2(PersonalizedSearchResult)
  implicit val fileLocationFormat = jsonFormat1(FileLocation)
  implicit val messageFormat = jsonFormat2(Message)

  implicit val alsParamGridFormat = jsonFormat4(ALSParamGrid)
  implicit val ndcgParamsFormat = jsonFormat2(NDCGParams)
  implicit val crossValidatorParamsFormat = jsonFormat1(CrossValidationParams)

  implicit val paramsFormat = jsonFormat3(Params)
}

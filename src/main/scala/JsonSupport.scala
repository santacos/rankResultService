import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val nonPersonalizedSearchResultFormat = jsonFormat2(NonPersonalizedSearchResult)
  implicit val personalizedSearchResultFormat = jsonFormat1(PersonalizedSearchResult)
}
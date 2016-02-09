import akka.http.scaladsl.server.Directives._

trait Route extends JsonSupport{
  val route =
    path("personalize"){
      post {
        entity(as[NonPersonalizedSearchResult]) { nonPersonalizedSearchResult =>
          complete{
            //PersonalizedSearchResult(nonPersonalizedSearchResult.items)
            (new SearchResultPersonalizer).personalize(nonPersonalizedSearchResult)
          }
        }
      }
  }
}


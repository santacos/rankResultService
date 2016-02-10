import akka.http.scaladsl.server.Directives._

trait Route extends JsonSupport{
  val route =
    path("personalize"){
      post {
        entity(as[NonPersonalizedSearchResult]) { nonPersonalizedSearchResult =>
          complete{
            val personalizer = new SearchResultPersonalizer
            personalizer.personalize(nonPersonalizedSearchResult)
          }
        }
      }
    }
}


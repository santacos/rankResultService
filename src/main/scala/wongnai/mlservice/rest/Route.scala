package wongnai.mlservice.rest
import akka.http.scaladsl.server.Directives._
import wongnai.mlservice.Spark.sqlContext
import wongnai.mlservice.rest.controller.PersonalizationController
import wongnai.mlservice.rest.controller.PersonalizationController.recommendationModel

/**
  * Created by ibosz on 24/3/59.
  */
trait Route extends JsonSupport {
  val route =
    path("personalize" / "rank"){
      post {
        entity(as[SearchResult]) { searchResult =>
          complete {
            PersonalizedSearchResult(searchResult.user, searchResult.items)
          }
        }
      }
    } ~
    path("personalize" / "train"){
      post {
        entity(as[SearchResult]) { searchResult =>
          complete {
            val path = "/Users/macbookair/Downloads/wongnai/log1.csv"

            PersonalizationController.train(path)
            "training request successful"
          }
        }
      } ~
      get {
        complete {
          val test = sqlContext.createDataFrame(Seq(
            (1807038, 208596, 1),
            (1456594, 174842, 3),
            (1802099, 192971, 1),
            (2119042, 193099, 1),
            (1617985, 191637, 1)
          )).toDF("user", "item", "rating")

          recommendationModel.transform(test).show()

          "look at log"
        }
      }
    }

}

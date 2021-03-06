package wongnai.mlservice.rest
import akka.http.scaladsl.server.Directives._
import wongnai.mlservice.Spark
import wongnai.mlservice.api.searchranking.{NDCGParams, CrossValidationParams, ALSParamGrid}
import wongnai.mlservice.rest.controller.PersonalizationController

import scala.concurrent.Future

/**
  * Created by ibosz on 24/3/59.
  */
trait Route extends JsonSupport {
  implicit val sc = Spark.sparkContext
  implicit val sqlc = Spark.sqlContext

  val route =
    path("personalize" / "rank") {
      post {
        entity(as[SearchResult]) { searchResult =>
          complete(PersonalizationController.rank(searchResult.user, searchResult.items))
        }
      }
    } ~
    path("personalize" / "train") {
      post {
        entity(as[FileLocation]) { fileLocation =>
          complete {
            import scala.concurrent.ExecutionContext.Implicits.global

            Future { PersonalizationController.train(fileLocation.path) }

            "request success"
          }
        }
      }
    } ~
    path("personalize" / "train-without-eval") {
      post {
        entity(as[FileLocation]) { fileLocation =>
          complete {
            import scala.concurrent.ExecutionContext.Implicits.global

            Future { PersonalizationController.trainWithoutEval(fileLocation.path) }

            "request success"
          }
        }
      }
    } ~
    path("personalize" / "model" / "save") {
      post {
        entity(as[FileLocation]) { fileLocation =>
          PersonalizationController.save(fileLocation.path)
          complete("save successful")
        }
      }
    } ~
    path("personalize" / "model" / "load") {
      post {
        entity(as[FileLocation]) { fileLocation =>
          PersonalizationController.load(fileLocation.path)
          complete("load successful")
        }
      }
    } ~
    path("personalize" / "model" / "status") {
      get {
        complete("status")
      }
    } ~
    path("personalize" / "model" / "setting") {
      get {
        complete(Params(
          PersonalizationController.alsParamGrid,
          PersonalizationController.crossValidationParams,
          PersonalizationController.ndcgParams
        ))
      }
    } ~
    path("personalize" / "model" / "setting" / "als") {
      post {
        entity(as[ALSParamGrid]) { alsParamGrid =>
          PersonalizationController.alsParamGrid = alsParamGrid
          complete(PersonalizationController.alsParamGrid)
        }
      }
    } ~
    path("personalize" / "model" / "setting" / "crossvalidator") {
      post {
        entity(as[CrossValidationParams]) { crossValidationParams =>
          PersonalizationController.crossValidationParams = crossValidationParams
          complete(PersonalizationController.crossValidationParams)
        }
      }
    } ~
    path("personalize" / "model" / "setting" / "ndcg") {
      post {
        entity(as[NDCGParams]) { ndcgParams =>
          PersonalizationController.ndcgParams = ndcgParams
          complete(PersonalizationController.ndcgParams)
        }
      }
    } ~
    path("personalize" / "model" / "summary") {
      get {
        complete(PersonalizationController.trainedModelResult.mkString("\n"))
      }
    } ~
    path("personalize" / "checkpoint") {
      post {
        entity(as[FileLocation]) { fileLocation =>
          complete {
            Spark.sparkContext.setCheckpointDir(fileLocation.path)

            s"checkpoint is set to ${fileLocation.path}"
          }
        }
      }
    }

}

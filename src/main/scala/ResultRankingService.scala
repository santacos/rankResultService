import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import scala.concurrent.Future
import scala.io.StdIn
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
  * Created by santacos on 2/9/2016 AD.
  */
object ResultRankingService extends App with Route {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val config = ConfigFactory.load()

  val bindingFuture =
    Http().bindAndHandle(
      route,
      config.getString("http.interface"),
      config.getInt("http.port"))

  println(s"server online at http://${config.getString("http.interface")}:${config.getInt("http.port")}")
  println("Press RETURN to stop...")
  StdIn.readLine()
  bindingFuture
    .flatMap(_.unbind())
    .onComplete(_ => system.terminate())

}


package wongnai.mlservice.rest

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory

/**
  * Created by ibosz on 24/3/59.
  */
object ServerBootstrap extends App with Route{
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

//  while(true) {}
//
//  bindingFuture
//    .flatMap(_.unbind())
//    .onComplete(_ => system.shutdown())

}

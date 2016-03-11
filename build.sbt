import _root_.sbtassembly.AssemblyPlugin.autoImport._
import _root_.sbtassembly.PathList

name := "ResultRankingService"

version := "1.0"

scalaVersion := "2.10.5"
scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

mainClass in Compile := Some("ResultRankingService")

libraryDependencies ++= {
  val akkaV       = "2.3.14"
  val akkaStreamV = "2.0.1"
  val scalaTestV  = "2.2.5"
  Seq(
    "com.typesafe.akka" %% "akka-actor"                           % akkaV,
    "com.typesafe.akka" %% "akka-stream-experimental"             % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-core-experimental"          % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-experimental"               % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-spray-json-experimental"    % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-testkit-experimental"       % akkaStreamV,
    "org.apache.spark"  %% "spark-core"                           % "1.6.0" % "provided",
    "org.apache.spark"  %% "spark-mllib"                          % "1.6.0",
    "org.scalatest"     %% "scalatest"                            % scalaTestV % "test"
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case _                             => MergeStrategy.first
}


    
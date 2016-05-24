import _root_.sbtassembly.AssemblyPlugin.autoImport._
import _root_.sbtassembly.PathList

name := "ResultRankingService"

version := "1.0"

scalaVersion := "2.10.5"
scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

mainClass in Compile := Some("wongnai.mlservice.Spark")

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
    "com.databricks"    %% "spark-csv"                            % "1.4.0",
    "org.apache.spark"  %% "spark-core"                           % "1.6.1" % "provided",
    "org.apache.spark"  %% "spark-mllib"                          % "1.6.1",
    "org.scalatest"     %% "scalatest"                            % scalaTestV % "test",
    "org.scalamock"     %% "scalamock-scalatest-support"          % "3.2.2" % "test",
    "com.datastax.spark" %% "spark-cassandra-connector"           % "1.6.0-M2",
    "com.datastax.spark" %% "spark-cassandra-connector-embedded"  % "1.6.0-M2"
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case _                             => MergeStrategy.first
}

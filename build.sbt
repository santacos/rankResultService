name := "ResultRankingService"

version := "1.0"

scalaVersion := "2.11.7"
scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= {
  val akkaV       = "2.3.14"
  val akkaStreamV = "2.0.3"
  val sparkV      = "1.6.0"
  val hadoopV     = "2.0.0-mr1-cdh4.4.0"

  val scalaTestV  = "2.2.5"
  val scalaMockV  = "3.2.2"

  Seq(
    "com.typesafe.akka" %% "akka-actor"                           % akkaV,
    "com.typesafe.akka" %% "akka-stream-experimental"             % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-core-experimental"          % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-experimental"               % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-spray-json-experimental"    % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-testkit-experimental"       % akkaStreamV,
    "org.apache.spark"  %% "spark-core"                           % sparkV,
    "org.apache.spark"  %% "spark-mllib"                          % sparkV,
    "org.apache.spark"  %% "spark-yarn"                           % sparkV,
    "org.scalatest"     %% "scalatest"                            % scalaTestV % "test",
    "org.scalamock"     %% "scalamock-scalatest-support"          % scalaMockV % "test"
  )
}

package wongnai.mlservice.rest.controller

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

/**
  * Created by ibosz on 1/6/59.
  */
class PersonalizationController$Test extends FunSuite with Matchers with BeforeAndAfterEach {
  val config = ConfigFactory.load()
  val sparkConfig: SparkConf = new SparkConf()
    .setAppName("SearchResultRanker")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", config.getString("cassandra.connection.host"))
    .set("spark.cassandra.auth.username"  , config.getString("cassandra.username"))
    .set("spark.cassandra.auth.password"  , config.getString("cassandra.password"))

  val sparkContext: SparkContext = new SparkContext(sparkConfig)
  val sqlContext: SQLContext = new SQLContext(sparkContext)

  override def beforeEach() {
    CassandraConnector(sparkConfig).withSessionDo { session =>
      session.execute(
        s"CREATE KEYSPACE IF NOT EXISTS wongnai " +
          "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 } " +
          "AND durable_writes = true;")

      session.execute(
        s"CREATE KEYSPACE IF NOT EXISTS wongnai_log " +
          "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 } " +
          "AND durable_writes = true;")

      session.execute(s"CREATE TABLE IF NOT EXISTS wongnai.user_features(" +
        "user_id int PRIMARY KEY, " +
        "features list<double> ); ")
      session.execute(s"CREATE TABLE IF NOT EXISTS wongnai.item_features(" +
        "item_id int PRIMARY KEY, " +
        "features list<double> ); ")
      session.execute(
        s"CREATE TABLE IF NOT EXISTS wongnai_log.entity_access2 " +
          "(key int, time timeuuid, entity text, entity_2 text, session text, " +
          "type text, unix_time bigint, user text, PRIMARY KEY (key, time));")

      session.execute(s"TRUNCATE wongnai.user_features")
      session.execute(s"TRUNCATE wongnai.item_features")
      session.execute(s"TRUNCATE wongnai_log.entity_access2")
    }
  }

  override def afterEach() {
    CassandraConnector(sparkConfig).withSessionDo { session =>
      session.execute(s"TRUNCATE wongnai.user_features")
      session.execute(s"TRUNCATE wongnai.item_features")
      session.execute(s"TRUNCATE wongnai_log.entity_access2")
    }
  }

  test("testTrainWithoutEval") {
    sparkContext.parallelize(Seq(
      (2016032912, "d76b7770-0173-11e6-a558-e13cbb0c8497", "2", null, "F57032D4A98460EF8A5A2DF91E2485A4", "1"),
      (2016032912, "d76b7770-0173-11e6-a558-e13cbb0c8492", "1", "2", "F57032D4A98460EF8A5A2DF91E2485A4", "1"),
      (2016041319, "20d78c50-0174-11e6-a558-e13cbb0c8097", "1", null, "F57032D4A98460EF8A5A2DF91E2485A4", "2"),
      (2016041319, "20d78c50-0174-11e6-a558-e13cbb0c8497", "2", "1", "F57032D4A98460EF8A5A2DF91E2485A4", "2"),
      (2016041320, "20d78c50-0174-11e6-a558-e13cbb0c8499", "4", null, "F57032D4A98460EF8A5A2DF91E2485A4", "3")
    )).saveToCassandra("wongnai_log", "entity_access2", SomeColumns("key", "time", "entity", "entity_2", "session", "user"))

    sparkContext.setCheckpointDir("~/spark/checkpoint")

    implicit val sc = sparkContext
    implicit val sqlc = sqlContext

    PersonalizationController.trainWithoutEval("")

    sparkContext.cassandraTable("wongnai", "user_features").collect should have length 3
    sparkContext.cassandraTable("wongnai", "item_features").collect should have length 3
  }

}

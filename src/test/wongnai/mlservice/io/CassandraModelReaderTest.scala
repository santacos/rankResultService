package wongnai.mlservice.io

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

/**
  * Created by ibosz on 17/5/59.
  */
class CassandraModelReaderTest extends FunSuite with BeforeAndAfterEach with Matchers {
  import testutil.FunSuiteSpark._

  override def beforeEach() {
    CassandraConnector(sparkConfig).withSessionDo { session =>
      session.execute(
        s"CREATE KEYSPACE IF NOT EXISTS wongnai " +
          "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 } " +
          "AND durable_writes = true;")

      session.execute(s"CREATE TABLE IF NOT EXISTS wongnai.recommendation(" +
        "user_id int, " +
        "item_id int, " +
        "score double, " +
        "PRIMARY KEY (user_id, item_id));")

      session.execute(s"CREATE TABLE IF NOT EXISTS wongnai.user_features(" +
        "user_id int PRIMARY KEY, " +
        "features list<double> ); ")

      session.execute(s"CREATE TABLE IF NOT EXISTS wongnai.item_features(" +
        "item_id int PRIMARY KEY, " +
        "features list<double> ); ")

      session.execute(s"TRUNCATE wongnai.recommendation")
      session.execute(s"TRUNCATE wongnai.user_features")
      session.execute(s"TRUNCATE wongnai.item_features")
    }
  }

  override def afterEach() {
    CassandraConnector(sparkConfig).withSessionDo { session =>
      session.execute(s"TRUNCATE wongnai.recommendation")
    }
  }

  test("get predictions with all available predictions") {
    sparkContext.parallelize(Seq(
      (1, 1, 2.0),
      (1, 2, 3.0),
      (1, 4, 5.0),
      (2, 1, 3.0),
      (2, 4, 1.0)
    )).saveToCassandra("wongnai", "recommendation", SomeColumns("user_id", "item_id", "score"))

    val modelReader =
      new CassandraModelReader(sparkContext, "wongnai", "recommendation")

    val predictions = modelReader.getPredictions(userId = 1, itemIds = List(1, 4))

    predictions should contain theSameElementsAs Array(
      (1, 1, 2.0),
      (1, 4, 5.0)
    )
  }

  test("get predictions with unavailable predictions") {
    sparkContext.parallelize(Seq(
      (1, 1, 2.0),
      (1, 2, 3.0),
      (1, 4, 5.0),
      (2, 1, 3.0),
      (2, 4, 1.0)
    )).saveToCassandra("wongnai", "recommendation", SomeColumns("user_id", "item_id", "score"))

    val modelReader =
      new CassandraModelReader(sparkContext, "wongnai", "recommendation")

    val predictions = modelReader.getPredictions(userId = 1, itemIds = List(1, 4, 100))

    predictions should contain theSameElementsAs Array(
      (1, 1, 2.0),
      (1, 4, 5.0),
      (1, 100, 0.0)
    )
  }

  test("get predictions from uncomputed user-item pairs") {
    sparkContext.parallelize(Seq(
      (1, Vector(2.0, 1.0, 4.0))
    )).saveToCassandra("wongnai", "user_features", SomeColumns("user_id", "features"))

    sparkContext.parallelize(Seq(
      (1, Vector(3.0, 2.0, 1.0)),
      (2, Vector(2.0, 1.0, 5.0))
    )).saveToCassandra("wongnai", "item_features", SomeColumns("item_id", "features"))

    val modelReader =
      new CassandraModelReader(sparkContext, "wongnai", "recommendation")

    val predictions = modelReader.getPredictions(userId = 1, itemIds = List(1, 2))

    predictions should contain theSameElementsAs Array(
      (1, 1, 12.0),
      (1, 2, 25.0)
    )
  }

  test("get predictions from uncomputed user-item pairs should save result to recommendation table"){
    sparkContext.parallelize(Seq(
      (1, Vector(2.0, 1.0, 4.0))
    )).saveToCassandra("wongnai", "user_features", SomeColumns("user_id", "features"))

    sparkContext.parallelize(Seq(
      (1, Vector(3.0, 2.0, 1.0)),
      (2, Vector(2.0, 1.0, 5.0))
    )).saveToCassandra("wongnai", "item_features", SomeColumns("item_id", "features"))

    val modelReader =
      new CassandraModelReader(sparkContext, "wongnai", "recommendation")

    modelReader.getPredictions(userId = 1, itemIds = List(1, 2))

    val recommendation = sparkContext
      .cassandraTable[(Int, Int, Double)]("wongnai", "recommendation").collect

    recommendation should contain theSameElementsAs Array(
      (1, 1, 12.0),
      (1, 2, 25.0)
    )
  }

  test("get predictions from computed user-item pairs if exists "){
    sparkContext.parallelize(Seq(
      (1, Vector(2.0, 1.0, 4.0))
    )).saveToCassandra("wongnai", "user_features", SomeColumns("user_id", "features"))

    sparkContext.parallelize(Seq(
      (1, Vector(3.0, 2.0, 1.0)),
      (2, Vector(2.0, 1.0, 5.0))
    )).saveToCassandra("wongnai", "item_features", SomeColumns("item_id", "features"))

    sparkContext.parallelize(Seq(
      (1, 1, 2.0),
      (1, 2, 3.0),
      (1, 4, 5.0),
      (2, 1, 3.0),
      (2, 4, 1.0)
    )).saveToCassandra("wongnai", "recommendation", SomeColumns("user_id", "item_id", "score"))

//    (1, 1, 12.0),
//    (1, 2, 25.0)

    val modelReader =
      new CassandraModelReader(sparkContext, "wongnai", "recommendation")

    val predictions = modelReader.getPredictions(userId = 1, itemIds = List(1, 2))


    predictions should contain theSameElementsAs Array(
      (1, 1, 2.0),
      (1, 2, 3.0)
    )
  }
}

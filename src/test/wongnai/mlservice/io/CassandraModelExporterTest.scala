package wongnai.mlservice.io

import com.datastax.spark.connector.cql.CassandraConnector
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers, Inspectors}
import com.datastax.spark.connector._

/**
  * Created by ibosz on 17/5/59.
  */
class CassandraModelExporterTest extends FunSuite with BeforeAndAfterEach with Matchers with Inspectors{
  import testutil.FunSuiteSpark._

  override def beforeEach() {
    CassandraConnector(sparkConfig).withSessionDo { session =>
      session.execute(
        s"CREATE KEYSPACE IF NOT EXISTS wongnai " +
          "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 } " +
          "AND durable_writes = true;")

      session.execute(s"CREATE TABLE IF NOT EXISTS wongnai.user_features(" +
        "user_id int PRIMARY KEY, " +
        "features list<double> ); ")
      session.execute(s"CREATE TABLE IF NOT EXISTS wongnai.item_features(" +
        "item_id int PRIMARY KEY, " +
        "features list<double> ); ")

      session.execute(s"TRUNCATE wongnai.user_features")
      session.execute(s"TRUNCATE wongnai.item_features")
    }
  }

  override def afterEach() {
    CassandraConnector(sparkConfig).withSessionDo { session =>
      session.execute(s"TRUNCATE wongnai.user_features")
      session.execute(s"TRUNCATE wongnai.item_features")
    }
  }

  test("successfully export user features") {
    val exporter = new CassandraModelExporter(
      sc = sparkContext,
      keyspace = "wongnai",
      userTable = "user_features",
      itemTable = "item_features"
    )

    val userFactors = sparkContext.parallelize(Seq(
      (1, Array(1.0, 1.0, 1.0)),
      (2, Array(2.0, 1.0, 3.0))
    ))

    val itemFactors = sparkContext.parallelize(Seq(
      (4, Array(1.0, 1.0, 1.0)),
      (5, Array(2.0, 1.0, 2.0))
    ))

    exporter.exportModel(rank = 3, userFactors, itemFactors)

    val recommendationTable = sparkContext
      .cassandraTable("wongnai", "user_features").collect
      .map(row => (
        row.get[Int]("user_id"),
        row.get[List[Double]]("features")))

    recommendationTable should have length 2


    forAll(recommendationTable){ case (userId: Int, features: List[Double]) =>
      userFactors.lookup(userId).head shouldBe features
    }
  }


  test("successfully export item features") {
    val exporter = new CassandraModelExporter(
      sc = sparkContext,
      keyspace = "wongnai",
      userTable = "user_features",
      itemTable = "item_features"
    )

    val userFactors = sparkContext.parallelize(Seq(
      (1, Array(1.0, 1.0, 1.0)),
      (2, Array(2.0, 1.0, 3.0))
    ))

    val itemFactors = sparkContext.parallelize(Seq(
      (4, Array(1.0, 1.0, 1.0)),
      (5, Array(2.0, 1.0, 2.0))
    ))

    exporter.exportModel(rank = 3, userFactors, itemFactors)

    val recommendationTable = sparkContext
      .cassandraTable("wongnai", "item_features").collect
      .map(row => (
        row.get[Int]("item_id"),
        row.get[List[Double]]("features")))

    recommendationTable should have length 2

    forAll(recommendationTable){
      case (itemId: Int, features: List[Double]) =>
        itemFactors.lookup(itemId).head shouldBe features
    }
  }
}

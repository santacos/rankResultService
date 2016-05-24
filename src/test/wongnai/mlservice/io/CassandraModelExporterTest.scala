package wongnai.mlservice.io

import com.datastax.spark.connector.cql.CassandraConnector
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}
import com.datastax.spark.connector._

/**
  * Created by ibosz on 17/5/59.
  */
class CassandraModelExporterTest extends FunSuite with BeforeAndAfterEach with Matchers {
  import testutil.FunSuiteSpark._

  override def beforeEach() {
    CassandraConnector(sparkConfig).withSessionDo { session =>
      session.execute(s"TRUNCATE wongnai.recommendation")
      session.execute(
        s"CREATE KEYSPACE IF NOT EXISTS wongnai " +
         "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 } " +
         "AND durable_writes = true;")
      session.execute(s"CREATE TABLE IF NOT EXISTS wongnai.recommendation(" +
        "user_id int, " +
        "item_id int, " +
        "score double, " +
        "PRIMARY KEY (user_id, item_id));")
    }
  }

  override def afterEach() {
    CassandraConnector(sparkConfig).withSessionDo { session =>
      session.execute(s"TRUNCATE wongnai.recommendation")
    }
  }

  test("successfully export the model") {
    val exporter = new CassandraModelExporter(
      sc = sparkContext,
      keyspace = "wongnai",
      table = "recommendation"
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
      .cassandraTable("wongnai", "recommendation").collect
      .map(row => (
        row.get[Int]("user_id"),
        row.get[Int]("item_id"),
        row.get[Double]("score")))

    recommendationTable should contain theSameElementsAs Array(
      (1, 4, 3.0),
      (1, 5, 5.0),
      (2, 4, 6.0),
      (2, 5, 11.0)
    )
  }
}

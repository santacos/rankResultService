package wongnai.mlservice.io.datasource

import com.datastax.spark.connector.{SomeColumns, _}
import com.datastax.spark.connector.cql.CassandraConnector
import org.scalatest.{Matchers, BeforeAndAfterEach, FunSuite}

/**
  * Created by ibosz on 12/5/59.
  */
class CassandraImporterTest extends FunSuite with BeforeAndAfterEach with Matchers {
  import testutil.FunSuiteSpark._

  override def beforeEach() {
    CassandraConnector(sparkConfig).withSessionDo { session =>
      session.execute(s"TRUNCATE wongnai_log.entity_access")
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS wongnai_log WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"CREATE TABLE IF NOT EXISTS wongnai_log.entity_access (key int, time timeuuid, entity text, entity_2 text, session text, type text, unix_time bigint, user text, PRIMARY KEY (key, time));")
    }
  }

  override def afterEach() {
    CassandraConnector(sparkConfig).withSessionDo { session =>
      session.execute(s"TRUNCATE wongnai_log.entity_access")
    }
  }

  test("ImportData with no exceptional case") {
    sparkContext.parallelize(Seq(
      (2016032912, "4942f9a0-f56b-11e5-af13-6b340b88d947","205850", null, "B14C31D0DCAB4ACF67A1C027021AE602", "55964")
    )).saveToCassandra("wongnai_log", "entity_access", SomeColumns("key", "time", "entity", "entity_2", "session", "user"))

    val logs = new CassandraImporter(sparkContext, "wongnai_log", "entity_access").importData().collect()

    // user, item, rating
    logs shouldBe Array((55964, 205850, 1))
  }

  test("ImportData with null user") {
    sparkContext.parallelize(Seq(
      (2016032912, "d76b7770-0173-11e6-a558-e13cbb0c8497", "1", null, "F57032D4A98460EF8A5A2DF91E2485A4", "1"),
      (2016032912, "d76b7770-0173-11e6-a558-e13cbb0c8492", "1", null, "F57032D4A98460EF8A5A2DF91E2485A4", null),
      (2016041319, "20d78c50-0174-11e6-a558-e13cbb0c8097", "1", null, "F57032D4A98460EF8A5A2DF91E2485A4", "2"),
      (2016041319, "20d78c50-0174-11e6-a558-e13cbb0c8497", "1", null, "F57032D4A98460EF8A5A2DF91E2485A4", null)
    )).saveToCassandra("wongnai_log", "entity_access", SomeColumns("key", "time", "entity", "entity_2", "session", "user"))

    val logs = new CassandraImporter(sparkContext, "wongnai_log", "entity_access").importData().collect()

    // user, item, rating
    logs should contain theSameElementsAs Array((1, 1, 1), (2, 1, 1))
  }

  test("ImportData with null entity"){
    sparkContext.parallelize(Seq(
      (2016032912, "d76b7770-0173-11e6-a558-e13cbb0c8497", null, null, "F57032D4A98460EF8A5A2DF91E2485A4", "1"),
      (2016032912, "d76b7770-0173-11e6-a558-e13cbb0c8492", null, "2", "F57032D4A98460EF8A5A2DF91E2485A4", "1"),
      (2016041319, "20d78c50-0174-11e6-a558-e13cbb0c8097", "1", null, "F57032D4A98460EF8A5A2DF91E2485A4", "2"),
      (2016041319, "20d78c50-0174-11e6-a558-e13cbb0c8497", "2", null, "F57032D4A98460EF8A5A2DF91E2485A4", "2"),
      (2016041320, "20d78c50-0174-11e6-a558-e13cbb0c8499", "4", null, "F57032D4A98460EF8A5A2DF91E2485A4", "3")
    )).saveToCassandra("wongnai_log", "entity_access", SomeColumns("key", "time", "entity", "entity_2", "session", "user"))

    val logs = new CassandraImporter(sparkContext, "wongnai_log", "entity_access").importData().collect()

    // user, item, rating
    logs should contain theSameElementsAs Array((2, 1, 1), (2, 2, 1), (3, 4, 1))

  }

  test("ImportData when entity2 not null"){
    sparkContext.parallelize(Seq(
      (2016032912, "d76b7770-0173-11e6-a558-e13cbb0c8497", "1", null, "F57032D4A98460EF8A5A2DF91E2485A4", "1"),
      (2016032912, "d76b7770-0173-11e6-a558-e13cbb0c8492", "1", "2", "F57032D4A98460EF8A5A2DF91E2485A4", "1"),
      (2016041319, "20d78c50-0174-11e6-a558-e13cbb0c8097", "1", null, "F57032D4A98460EF8A5A2DF91E2485A4", "2"),
      (2016041319, "20d78c50-0174-11e6-a558-e13cbb0c8497", "2", "3", "F57032D4A98460EF8A5A2DF91E2485A4", "2")
    )).saveToCassandra("wongnai_log", "entity_access", SomeColumns("key", "time", "entity", "entity_2", "session", "user"))

    val logs = new CassandraImporter(sparkContext, "wongnai_log", "entity_access").importData().collect()

    // user, item, rating
    logs should contain theSameElementsAs Array((1, 1, 1), (1, 2, 1), (2, 1, 1), (2, 3, 1))

  }

  test("ImportData count duplicated user-item"){
    sparkContext.parallelize(Seq(
      (2016032912, "d76b7770-0173-11e6-a558-e13cbb0c8497", "2", null, "F57032D4A98460EF8A5A2DF91E2485A4", "1"),
      (2016032912, "d76b7770-0173-11e6-a558-e13cbb0c8492", "1", "2", "F57032D4A98460EF8A5A2DF91E2485A4", "1"),
      (2016041319, "20d78c50-0174-11e6-a558-e13cbb0c8097", "1", null, "F57032D4A98460EF8A5A2DF91E2485A4", "2"),
      (2016041319, "20d78c50-0174-11e6-a558-e13cbb0c8497", "2", "1", "F57032D4A98460EF8A5A2DF91E2485A4", "2"),
      (2016041320, "20d78c50-0174-11e6-a558-e13cbb0c8499", "4", null, "F57032D4A98460EF8A5A2DF91E2485A4", "3")
    )).saveToCassandra("wongnai_log", "entity_access", SomeColumns("key", "time", "entity", "entity_2", "session", "user"))

    val logs = new CassandraImporter(sparkContext, "wongnai_log", "entity_access").importData().collect()

    // user, item, rating
    logs should contain theSameElementsAs Array((1, 2, 2), (2, 1, 2), (3, 4, 1))

  }



}

package wongnai.mlservice.preprocess

import org.apache.spark.sql.Row
import org.scalatest.Matchers
import testutil.FunSuiteSpark

import scala.util.matching.Regex

/**
  * Created by ibosz on 22/3/59.
  */
class RDDStringToDataFrameConverterTest extends FunSuiteSpark with Matchers {
  import testutil.FunSuiteSpark._

  test("DataFrame columns' name should be as it is predefined") {
    implicit val dataFrameConstructionContext = sqlContext

    val toTuple = (listOfMatched: List[String]) => listOfMatched match {
      case List(user, item, score) => (user, item, score)
    }
    val pattern = new Regex("(\\w)(\\w)(\\w)")
    val columns = Seq("user", "item", "score")

    val dataFrameConstructor =
      new RDDStringToDataFrameConverter
        [(String, String, String)](pattern, columns, toTuple)

    val rdd = sparkContext.parallelize(List("aaa", "bbb", "ccc"))
    val dataFrame = dataFrameConstructor.createDataFrame(rdd)

    dataFrame.schema.fieldNames shouldBe columns.toArray
  }

  test("type of each column should convert correctly") {
    implicit val dataFrameConstructionContext = sqlContext

    val toTuple = (listOfMatched: List[String]) => listOfMatched match {
      case List(user, item, score) => (user.toInt, item.toInt, score.toDouble)
    }
    val pattern = new Regex("(\\d+)::(\\d+)::(\\d*.\\d+)")
    val columns = Seq("user", "item", "score")

    val dataFrameConstructor =
      new RDDStringToDataFrameConverter
        [(Int, Int, Double)](pattern, columns, toTuple)

    val rdd = sparkContext.parallelize(List(
      "1::2::2.4", "2::4::5.6", "5::6::1.2"
    ))

    val dataFrame = dataFrameConstructor.createDataFrame(rdd)

    dataFrame.collect().foreach(row => row match {
      case Row(user, item, score) =>
        user.getClass.getName shouldBe "java.lang.Integer"
        item.getClass.getName shouldBe "java.lang.Integer"
        score.getClass.getName shouldBe "java.lang.Double"
    })
  }

  test("create DataFrame correctly") {
    implicit val dataFrameConstructionContext = sqlContext

    val toTuple = (listOfMatched: List[String]) => listOfMatched match {
      case List(user, item, score) => (user.toInt, item.toInt, score.toDouble)
    }
    val pattern = new Regex("(\\d+)::(\\d+)::(\\d*.\\d+)")
    val columns = Seq("user", "item", "score")

    val dataFrameConstructor =
      new RDDStringToDataFrameConverter
        [(Int, Int, Double)](pattern, columns, toTuple)

    val rdd = sparkContext.parallelize(List(
      "1::2::2.4", "2::4::5.6", "5::6::1.2"
    ))

    val dataFrame = dataFrameConstructor.createDataFrame(rdd)

    dataFrame.collect() shouldBe Array(
      Row(1, 2, 2.4),
      Row(2, 4, 5.6),
      Row(5, 6, 1.2)
    )
  }

  test("filter out unmatched string") {
    implicit val dataFrameConstructionContext = sqlContext

    val toTuple = (listOfMatched: List[String]) => listOfMatched match {
      case List(user, item, score) => (user.toInt, item.toInt, score.toDouble)
    }
    val pattern = new Regex("(\\d+)::(\\d+)::(\\d*.\\d+)")
    val columns = Seq("user", "item", "score")

    val dataFrameConstructor =
      new RDDStringToDataFrameConverter
        [(Int, Int, Double)](pattern, columns, toTuple)

    val rdd = sparkContext.parallelize(List(
      "1::2::2.4", "unmatched stuffs", "2::4::5.6", "5::6::1.2", ""
    ))

    val dataFrame = dataFrameConstructor.createDataFrame(rdd)

    dataFrame.collect() shouldBe Array(
      Row(1, 2, 2.4),
      Row(2, 4, 5.6),
      Row(5, 6, 1.2)
    )
  }

}

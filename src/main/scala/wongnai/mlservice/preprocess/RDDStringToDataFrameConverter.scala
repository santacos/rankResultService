package wongnai.mlservice.preprocess

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.matching.Regex
/**
  * Created by ibosz on 22/3/59.
  */
class RDDStringToDataFrameConverter[ColumnTypeInTuple <: Product :TypeTag:ClassTag](
    val pattern: Regex,
    val outputCols: Seq[String],
    val capturedListToTuple: List[String] => ColumnTypeInTuple
  )(implicit constructionContext: SQLContext) {

  def createDataFrame(rdd: RDD[String]): DataFrame = {
    import constructionContext.implicits._
    val reg = pattern
    val toTuple = capturedListToTuple

    rdd
      .map(reg.unapplySeq(_) match {
        case Some(list) => toTuple(list) })
      .toDF(outputCols: _*)
  }
}

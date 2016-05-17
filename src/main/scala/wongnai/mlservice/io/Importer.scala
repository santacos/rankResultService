package wongnai.mlservice.io

import org.apache.spark.rdd.RDD

/**
  * Created by ibosz on 12/5/59.
  */
trait Importer {
  def importData(): RDD[_]
}

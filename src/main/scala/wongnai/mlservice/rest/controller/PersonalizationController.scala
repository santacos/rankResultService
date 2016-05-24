package wongnai.mlservice.rest.controller

import org.apache.spark.ml.Model
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.Row
import wongnai.mlservice.Spark
import wongnai.mlservice.Spark._
import wongnai.mlservice.api.searchranking.{ALSParamGrid, CrossValidationParams, NDCGParams, model}
import wongnai.mlservice.io.{CassandraImporter, CassandraModelExporter, CassandraModelReader}
import wongnai.mlservice.rest.PersonalizedSearchResult

/**
  * Created by ibosz on 24/3/59.
  */
object PersonalizationController {
  var recommendationModel: Model[_] = _

  var alsParamGrid: ALSParamGrid = ALSParamGrid(
    maxIter = Array(20),
    rank = Array(20),
    alpha = Array(0.01, 0.1, 0.5),
    regParam = Array(10.0, 100.0, 1000.0)
  )
  var ndcgParams = NDCGParams(recommendingThreshold = 10.0, k = 2 )
  var crossValidationParams = CrossValidationParams(numFolds = 3)

  var trainedModelResult = List[String]()

  def train(sourcePath: String): Unit = {
    import Spark.sqlContext.implicits._
    val dataset = new CassandraImporter(sparkContext, "wongnai_log", "entity_access")
      .importData()
      .toDF("user","item","rating")

    recommendationModel =
      model.construct(dataset, alsParamGrid, ndcgParams, crossValidationParams)

    dataset.unpersist()
  }

  def trainWithoutEval(sourcePath: String): Unit = {
    import Spark.sqlContext.implicits._
    val dataset = new CassandraImporter(sparkContext, "wongnai_log", "entity_access")
      .importData()
      .toDF("user","item","rating")

    val model = new ALS()
      .setImplicitPrefs(true)
      .setRank(100)
      .setMaxIter(20)
      .setAlpha(200.0)
      .setRegParam(0.1)
      .fit(dataset)

    val factorDFToRDD: PartialFunction[Row, (Int, Array[Double])] = {
      case Row(id: Int, factors: Seq[Float]) => (id, factors.toArray.map(_.toDouble))
    }

    new CassandraModelExporter(sparkContext, "wongnai", "recommendation")
        .exportModel(
          model.rank,
          model.userFactors.map(factorDFToRDD),
          model.itemFactors.map(factorDFToRDD))
  }

  def rank(user: Int, items: List[Int]): PersonalizedSearchResult = {
    val scoredItems = new CassandraModelReader(sparkContext, "wongnai", "recommendation")
        .getPredictions(user, items)
        .map{ case (user, item, score) => Rating(user, item, score.toFloat) }

    val positiveItems = scoredItems
      .filter(_.rating > 0)
      .sortBy(- _.rating)
      .map(_.item)

    val notToRankItems = items diff positiveItems

    PersonalizedSearchResult(user, positiveItems ++ notToRankItems)
  }

  def save(path: String): Unit = {
    recommendationModel.asInstanceOf[CrossValidatorModel].save(path)
  }

  def load(path: String): Unit = {
    recommendationModel = CrossValidatorModel.load(path)
  }
}

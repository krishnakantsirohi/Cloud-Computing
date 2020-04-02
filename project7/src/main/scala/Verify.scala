
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Verify {
  case class Rating(userId: Double, artistId: Double, weight: Double)
  def parseRating(str: String): Rating = {
    val fields = str.split("\t")
    assert(fields.size == 3)
    Rating(fields(0).toDouble, fields(1).toDouble, fields(2).toDouble)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LastFM")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val SQLContext = new SQLContext(sc)
    import SQLContext.implicits._
    //val user_artist = sc.textFile(args(0)).map(l1 => ((l1.split("\t")(0).toLong,l1.split("\t")(1).toLong),l1.split("\t")(2).toLong)).collect()
    val ratings = sc.textFile(args(0)).map(parseRating).toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("artistId")
      .setRatingCol("weight")
    val model = als.fit(training)
    val predictions = model.transform(test)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse:Double = evaluator.evaluate(predictions)
    println("Root-mean-square error = "+rmse)
  }
}
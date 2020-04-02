import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KMeans {
  type Point = (Double,Double)

  var centroids: Array[Point] = Array[Point]()
  var points: Array[Point] = Array[Point]()

  def main(args: Array[ String ]) {
    /* ... */
    val conf = new SparkConf().setAppName("KMeans")

    val sc = new SparkContext(conf)

    centroids = sc.textFile(args(1)).map(line => (line.split(",")(0).toDouble,line.split(",")(1).toDouble)).collect()
    points = sc.textFile(args(0)).map(line => (line.split(",")(0).toDouble,line.split(",")(1).toDouble)).collect()

    def dist(point: Point) ={
      var min_dis: Double = 10000
      var temp_dis:Double = 0
      var center:Point = new Point(0,0)

      for (i <- 0 to centroids.length-1 )
      {
        temp_dis = Math.abs(Math.pow(point._1-centroids(i)._1,2)+Math.pow(point._2-centroids(i)._2,2))
        if (temp_dis<min_dis)
          {
            min_dis=temp_dis
            center = centroids(i)
          }
      }
      (center,point)
    }
    def newcentroids(point:Array[((Double,Double),(Double,Double))]): (Double,Double) ={
      var newCenter: Point = new Point(0,0)
      var x:Double = 0
      var y:Double = 0
      var len = point.length
      for (i<-0 to point.length-1) {
        x = x + point(i)._2._1
        y = y + point(i)._2._2
      }
      x = x / len
      y = y / len
      newCenter = (x,y)
      newCenter
    }

    for ( i <- 1 to 5 ) {
      centroids = points.map(point1 => dist(point1)).groupBy(point2=>point2._1).map(point3=>(newcentroids(point3._2))).toArray
      }
    centroids.foreach(println)
    sc.stop()
  }
}
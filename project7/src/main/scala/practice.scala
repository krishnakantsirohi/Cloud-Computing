import org.apache.spark.{SparkConf, SparkContext}

object practice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LastFM")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val courses = sc.textFile("courses.txt").map(l => (l.split(",")(0),l.split(",")(1)))
    val enrollment = sc.textFile("enrollment.txt").map(l => (l.split(",")(1),l.split(",")(2).toLong))
    val gb = courses.join(enrollment).groupByKey().collect()
    val bc_sum = sc.broadcast(courses.join(enrollment).reduceByKey((a,b)=>(a._1,a._2+b._2)).mapValues(l => (l._1,l._2.toLong)).collect())
    def func(code:String, count:Long) ={
      var result:Float = 0
      for (i <- bc_sum.value.indices)
        {
          if (code == bc_sum.value(i)._1)
            result = bc_sum.value(i)._2._2/count
        }
      (code,result)
    }
    val result = gb.map(l => func(l._1,l._2.size))
    result.foreach(println(_))
  }
}

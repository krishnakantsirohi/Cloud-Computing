import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {

  var num = 1
  val depth = 6
  var a = 0
  val neg:Long = -1

  def Vert(str:Array[String]): (Long,Long,List[Long]) ={
    var centroidID:Long = -1
    if(num<=10){
      num = num+1
      centroidID = str(0).toLong
    }
    (str(0).toLong,centroidID,str.tail.map(_.toString.toLong).toList)
  }

  def func1(vert:(Long,Long,List[Long])):List[(Long,Either[(Long,List[Long]),Long])]={
      var t1 = List[(Long,Either[(Long,List[Long]),Long])]()//((vert._1,Left(vert._2,vert._3)))
      if(vert._2>0) {vert._3.foreach(x => {t1 = (x,Right(vert._2))::t1})}
      t1 = (vert._1,Left(vert._2,vert._3))::t1
    t1
  }

  def reduce(vertex: (Long,Iterable[(Either[(Long,List[Long]),Long])])):(Long,Long,List[Long])={
    var adjacent:List[Long] = List[Long]()
    var cluster:Long = -1
    for (p <- vertex._2){
      p match{
        case Right(c) => {cluster=c}
        case Left((c,adj)) if (c>0)=> return (vertex._1,c,adj)
        case Left((neg,adj)) => adjacent=adj
      }
    }
    return (vertex._1,cluster,adjacent)
  }

  def main ( args: Array[ String ] ) :Unit = {
    val conf = new SparkConf().setAppName("Partition")
    //conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    var graph = sc.textFile(args(0)).map(line => Vert(line.split(",")))

    for (i <- 1 to depth){
      graph = graph.flatMap(vertex => func1(vertex)).groupByKey().map(vertex => reduce(vertex))
    }

    var res = graph.map(h => (h._2,1))
    var result = res.reduceByKey(_+_).collect()

    //res.foreach(println)
    //println(result.count())
    result.foreach(println)
  }
}

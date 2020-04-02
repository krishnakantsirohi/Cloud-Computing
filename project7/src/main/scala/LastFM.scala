import org.apache.spark.{SparkConf, SparkContext}

object LastFM {

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("LastFM")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val user_artist = sc.textFile(args(0)).map(l1 => ((l1.split("\t")(0).toLong,l1.split("\t")(1).toLong),l1.split("\t")(2).toLong))
    val user_tagged_artist = sc.textFile(args(1)).map(l1 => ((l1.split("\t")(0).toLong,l1.split("\t")(1).toLong),l1.split("\t")(2).toLong))
    val user_friends = sc.textFile(args(2)).map(l1 => (l1.split("\t")(0).toLong,l1.split("\t")(1).toLong))
    val bc_user_tagged_artist = sc.broadcast(user_tagged_artist.collect())
    val threshold = user_artist.map(line => (line._1._1,(line._2,1))).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues(l => l._1/l._2)
    val bc_threshold = sc.broadcast(threshold.collect())
    /*
     * Returns the userid, artistid, weight and threshold weight for each tuple.
     */
    def func1(uid: Long, aid: Long , w: Long) ={
      var songs = List[(Long,Long,Long,Long)]()
      for (i <- bc_threshold.value.indices)
        {
          if (bc_threshold.value(i)._1==uid)
            {
              if (bc_threshold.value(i)._2*1.75 <= w)
                {
                  songs ::= (bc_threshold.value(i)._1,aid,w,bc_threshold.value(i)._2)
                }
            }
        }
      songs
    }
    val user_artist_filtered = user_artist.flatMap(l1 => func1(l1._1._1,l1._1._2,l1._2)).sortBy(_._1.toLong).distinct()
    val bc_user_artist_filtered = sc.broadcast(user_artist_filtered.collect())

    /*
     * Returns the userid, artistid, weight, tagID and threshold weight for each tuple.
     */
    def func2(uid:Long, aid:Long, wt:Long, th: Long)={
      var songs = List[(Long,Long,Long,Long,Long)]()
      for (i <- bc_user_tagged_artist.value.indices)
        {
          if (bc_user_tagged_artist.value(i)._1._1==uid && bc_user_tagged_artist.value(i)._1._2==aid)
            {
              songs ::= (uid, aid, wt, bc_user_tagged_artist.value(i)._2, th)
            }
        }
      songs
    }
    val user_artist_tagged_filtered = user_artist_filtered.flatMap(l1 => func2(l1._1,l1._2,l1._3,l1._4)).sortBy(_._1).distinct()

    /*
     * Returns the userid, artistid, weight and tagID weight for each tuple.
     */
    def func3(uid:Long, aid:Long, wt:Long)={
      var songs = List[(Long,Long,Long,Long)]()
      for (i <- bc_user_tagged_artist.value.indices)
        {
          if (bc_user_tagged_artist.value(i)._1._1==uid && bc_user_tagged_artist.value(i)._1._2==aid)
          {
            songs ::= (uid, aid, wt, bc_user_tagged_artist.value(i)._2)
          }
        }
      songs
    }
    println("loading tag data...")
    val user_tagged_artists = user_artist_filtered.flatMap(l1 => func3(l1._1,l1._2,l1._3)).sortBy(_._3).distinct()
    val bc_user_tagged_artists = sc.broadcast(user_tagged_artists.collect())

    def func4(uid:Long, tagID:Long, threshold:Long)={
      var playlist = List[(Long,Long,Long,Long,Long)]()
      for (i <- bc_user_tagged_artists.value.indices)
        {
          if (bc_user_tagged_artists.value(i)._4==tagID && bc_user_tagged_artists.value(i)._3>=threshold*1.75)
            playlist ::= (uid,bc_user_tagged_artists.value(i)._2,tagID,bc_user_tagged_artists.value(i)._3,threshold)
        }
      playlist
    }
    println("Filtering with other playlist")
    var intermediate_playlist = user_artist_tagged_filtered.flatMap(l1 => func4(l1._1,l1._4, l1._5)).sortBy(_._4).distinct()

    def func5(uid: Long, fid:Long) ={
      var fr = List[(Long,Long,Long,Long,Long)]()
      for (i <- bc_user_artist_filtered.value.indices)
        {
          if (bc_user_artist_filtered.value(i)._1==fid)
            {
              fr ::= (uid,fid,bc_user_artist_filtered.value(i)._1,bc_user_artist_filtered.value(i)._2,bc_user_artist_filtered.value(i)._3)  //(uid,fid,uid,aid,wt)
            }
        }
      fr
    }
    println("Filtering with friends playlist")
    val bc_user_friends_playlist = sc.broadcast(user_friends.flatMap(l1 =>  func5(l1._1,l1._2)).distinct().collect())

    def func6(uid:Long,aid:Long,tid:Long,wt:Long,th:Long)={
      var playlist = List[(Long,Long,Long)]()
      for (i <- bc_user_friends_playlist.value.indices)
        {
          if (bc_user_friends_playlist.value(i)._1==uid && bc_user_friends_playlist.value(i)._4==aid && bc_user_friends_playlist.value(i)._5>=th*1.75)
          playlist ::= (uid,aid,tid)
        }
      playlist
    }

    intermediate_playlist = intermediate_playlist.distinct().sortBy(_._4)
    val final_playlist = intermediate_playlist.flatMap(l1 => func6(l1._1,l1._2,l1._3,l1._4,l1._5)).distinct().sortBy(_._1)
    final_playlist.saveAsTextFile(args(3))
    final_playlist.foreach(println(_))
    println("Completed!")
  }
}
package pr

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object rdd{
  def main(args: Array[String]): Unit = {
    val startTimeMillis = System.currentTimeMillis()

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 1) {
      logger.error("Usage:\npr.rdd <K>")
      System.exit(1)
    }

    val k = args(0).toInt // no. of vertices

    val spark = SparkSession
      .builder
      .appName("PageRankRDD")
      .getOrCreate()

    val noOfNodes = k * k
    val nodesList = List.range(1, noOfNodes + 2)
    //nodesList:1 to 10001
    //println(nodesList)
    val initialPR = 1.0 / noOfNodes
    val alpha = 0.15

    val edges = toPairs(nodesList, k)
    //println(edges);
    val ranks = List.range(0, noOfNodes + 1).map(x => if (x == 0) (x.toString, 0.0) else (x.toString, initialPR))

    val graphRDD = spark.sparkContext.parallelize(edges).cache()
    var rankRDD = spark.sparkContext.parallelize(ranks)
    //println(rankRDD.lookup("0").head)

    val iters = 10
    
    for (i <- 1 to iters) {
      val newRDD = graphRDD.join(rankRDD)
      //println(newRDD.lookup("2").head);
      val tempRDD = newRDD.flatMap(joinPair =>
        if (joinPair._1.toInt % k == 1)
          List((joinPair._1, 0.0), joinPair._2) // the first one of each k nodes get 0.0 pagerank
        else
          List(joinPair._2))
       
      val temp2RDD = tempRDD.groupByKey().map(x => {//println(x._1)
        (x._1, x._2.sum)})

      val delta = temp2RDD.lookup("0").head
      //println(temp2RDD.lookup("1").head)
      //add the mass loss to every node
      rankRDD = temp2RDD.map(v => {
        if (v._1.equals("0")) {
          (v._1, v._2)
        } else {
          (v._1, alpha*initialPR + (1-alpha) * (v._2 + delta * initialPR))
        }
      })//.cache()

      logger.warn("PageRank sum for iteration " + i + " : " + rankRDD.filter(_._1 != "0").map(_._2).sum())
      println("PageRank sum for iteration " + i + " : " + rankRDD.filter(_._1 != "0").map(_._2).sum())
      if (i < 4) {
        println(rankRDD.toDebugString)
      }
    }

    logger.warn("*****************************************************************************************************")

    logger.warn("Execution Time:" + (System.currentTimeMillis() - startTimeMillis) + "ms.")

    logger.warn("*****************************************************************************************************")

    val topKRDDByPR = rankRDD.filter(_._1 != "0").takeOrdered(k)(Ordering[Double].reverse.on { x => x._2 })
    logger.warn("Top K Page Rank values:")
    println("Top K Page Rank values:")
    logger.warn(topKRDDByPR.foreach(println))

    logger.warn("*****************************************************************************************************")

    val topKRDD = rankRDD.takeOrdered(k + 1)(Ordering[Int].on { x => x._1.toInt })
    logger.warn("Top K values:")
    println("Top K values:")
    logger.warn(topKRDD.foreach(println))

    spark.stop()
  }

  def toPairs(a: Seq[Int], k: Int): List[(String, String)] = {
    a.sliding(2).map(x => {
      if (x.head % k == 0) {
        (x.head.toString, "0")
      } else {
        (x.head.toString, x.tail.head.toString)
      }
    }).toList
  }
  
}
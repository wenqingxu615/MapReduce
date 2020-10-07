package Repjoin

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object Rddjoin{
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val MAX_FILTER = 20000
    if (args.length != 2) {
      logger.error("Usage:\nRepjoin.Rddjoin Main <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Repjoin RDD")
    conf.set("spark.logLineage", "true")

    val sc = new SparkContext(conf)

    val lineRDD = sc.textFile(args(0))

    val filterRDD = lineRDD.filter(line => {
      val splitVals = line.split(",")
      val from = splitVals(0)
      val to = splitVals(1)

      from.toLong < MAX_FILTER && to.toLong < MAX_FILTER
    })

    val RDD1 = filterRDD.map(line => {
      val splitVals = line.split(",")
      val from = splitVals(0)
      val to = splitVals(1)

      (from, to)
    })

    val RDD2 = filterRDD.map(line => {
      val splitVals = line.split(",")

      val from = splitVals(0)
      val to = splitVals(1)
      

      (from, to)
    })

    val accum = sc.longAccumulator("Triangle Accumulator")

    val edgesRDD = RDD1.collect().groupBy { case (from, to) => to }
    //use the broadcast method to broadcaset the edges
    val broadCastVal = sc.broadcast(edgesRDD)

    val path2edges = RDD2.mapPartitions(iter => {
      //for each edges in RDD2
      iter map { case (fromNode, toNode) => {
        if (broadCastVal.value.get(fromNode).isDefined) {
          
          val z: Array[(String, String)] = broadCastVal.value(fromNode)
          // z= all the possible "toNodes" for each fromNode in RDD2
          z.foreach { case (zFrom, zTo) => {
            if (broadCastVal.value.get(zFrom).isDefined) {
              val x: Array[(String, String)] = broadCastVal.value(zFrom)
              // x= all the possible "toNodes" for each fromNode in z
              x.foreach { case (f, t) => {
                if (toNode.equals(f)) {
                  accum.add(1)
                }
              }
              }
            }
          }
          }
        }
      }
      }
    }, preservesPartitioning = true)

    //path2edges.collect().map(println)
    println("Triangle:", accum.value / 3)

  }
}
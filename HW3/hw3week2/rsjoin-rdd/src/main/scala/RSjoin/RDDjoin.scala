package RSjoin

import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDjoin{
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val MAX_FILTER = 20000
    if (args.length != 2) {
      logger.error("Usage:\nRSjoin.RDDjoin <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RDDjoin")
    conf.set("spark.logLineage", "true")

    val sc = new SparkContext(conf)

    val lineRDD = sc.textFile(args(0))

    val filterRDD = lineRDD.filter(line => {
      val users = line.split(",")
      val from = users(0)
      val to = users(1)

      from.toLong < MAX_FILTER && to.toLong < MAX_FILTER
    })

    val fromRDD = filterRDD.map(line => {
      val users = line.split(",")
      val from = users(0).toLong
      val to = users(1).toLong

      (from, to)
    })

    val toRDD = filterRDD.map(line => {
      val users = line.split(",")

      val to = users(1).toLong
      val from = users(0).toLong

      (to, from)
    })

    val answer = sc.longAccumulator("Triangle")

    val path2 = joinNodes(toRDD, fromRDD).map(_._2).filter(line => line._1 != line._2)
    joinNodes(path2, toRDD).map(_._2).foreach { x => if (x._1 == x._2) answer.add(1) }

    println("Triangle: " + answer.value / 3)
    //val intRdd= sc.parallelize(Seq(answer.value / 3))   
    //intRdd.saveAsTextFile(args(1))
    
  }

  def joinNodes(fromRDD: RDD[(Long, Long)],
                toRDD: RDD[(Long, Long)]): RDD[(Long, (Long, Long))] = {
    val joinedRDD = fromRDD.join(toRDD)
    joinedRDD
  }
}
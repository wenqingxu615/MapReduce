package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RDD_F {
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nweek1.RDD_F <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RDD_F")
    val sc = new SparkContext(conf)
    val file:RDD[String] = sc.textFile(args(0))
    val user: RDD[String] = file.flatMap(_.split(",").lastOption)
    val counts: RDD[(String,Int)] = user.map(user => (user,1))
    val results: RDD[(String,Int)] = counts.foldByKey(0)(_+_)
    println("info: ", results.toDebugString)
    results.saveAsTextFile(args(1))
  }
}
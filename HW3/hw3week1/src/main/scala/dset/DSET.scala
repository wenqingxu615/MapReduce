package dset

import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager
import org.apache.log4j.Level
//import org.apache.spark.rdd.RDD

object DSET{
  def main(args:Array[String]){
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("DSET")
      .getOrCreate()
    import sparkSession.implicits._
    val data = sparkSession.read.text(args(0)).as[String]
    val user = data.flatMap(r => r.split(",").lastOption)
    val counts = user.groupBy("Value")
                     .count().repartition(1)                
    counts.explain(true)
    counts.write.csv(args(1))
  }
}
package RSjoin

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import scala.collection.mutable.ListBuffer


object DSETjoin{
  // schema for twitter follower dataset
  case class TwitterFollowers(follower_id: Long, user_id: Long)

  val MAX_FILTER = 20000

  /** Main method */
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder.master("yarn")
      .appName("DSET rsjoin")
      .config("spark.logLineage", "true")
      .getOrCreate()

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nRSjoin.DSETjoin <input dir> <output dir> <combine type>")
      System.exit(1)
    }

    groupByDataset(args(0), args(1), spark)
  }

  def groupByDataset(input: String, output: String, spark: SparkSession) { // << add this
    import spark.implicits._

    val customSchema = StructType(Array(
      StructField("follower_id", LongType, nullable = false),
      StructField("user_id", LongType, nullable = false)))

    val followersDS = spark.read.format("csv").schema(customSchema).
      load(input)
      .where($"follower_id" < MAX_FILTER && $"user_id" < MAX_FILTER)

    val path2DS: Dataset[(Row, Row)] =
      followersDS.as("left").joinWith(followersDS.as("right"),
        $"left.user_id" === $"right.follower_id")

    val triangleDS: Dataset[((Row, Row), Row)] =
      path2DS.as("a").joinWith(followersDS.as("b"),
        $"a._1.follower_id" === $"b.user_id" && $"a._2.user_id" === $"b.follower_id")

    print("Triangle: " + triangleDS.count() / 3)
    
  }

}
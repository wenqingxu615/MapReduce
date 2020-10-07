package Repjoin

import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DSETjoin{
   case class TwitterFollowers(follower_id: Long, user_id: Long)
  val MAX_FILTER = 20000

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Rep DSET join")
      .config("spark.logLineage", "true")
      .getOrCreate()

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nRepjoin.DSETjoin <input dir> <output dir> <combine type>")
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

    val path2DS: DataFrame =
          followersDS.as("a").join(broadcast(followersDS).as("b"),
            $"a.user_id" === $"b.follower_id").select($"a.follower_id", $"b.user_id")

    val triangleDS: Dataset[(Row, Row)] =
      path2DS.as("a").joinWith(followersDS.as("b"),
        $"a.follower_id" === $"b.user_id" && $"a.user_id" === $"b.follower_id")

    print("Triangle: " + triangleDS.count()/3)
  }

}
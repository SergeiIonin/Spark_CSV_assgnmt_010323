import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.jdk.CollectionConverters.IteratorHasAsScala

object Spark_CSV_Assignment extends App {

  val config: Config = ConfigFactory.load("application.conf")

  val inputPath  = config.getString("input-path")
  val outputPath = config.getString("output-path")

  val sparkSession = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val firstDF: DataFrame = sparkSession.read
    .format("csv")
    .option("InferSchema", "true")
    .load(inputPath)

  LazyList.from(firstDF.toLocalIterator().asScala)

  lazy val content = LazyList.from(LazyList.from(firstDF.toLocalIterator().asScala).map(row => row.get(0) -> row.get(1))
    .groupMapReduce(_._1){
      case _ -> v => v -> 1
    } {
      case (k1 -> v1, _ -> v2) => (k1, v1 + v2)
    })

  lazy val contentFiltered = content.collect {
    case (key, (value, occurence)) if occurence % 2 != 0 => s"$key" -> s"$value"
  }

  sparkSession.createDataFrame(contentFiltered).write.csv(outputPath)

  val dfS3 = sparkSession.read.load("s3a://bucket.280223.serg/myFile")

  println("------" * 10)
  println(s"dfs3 = ${dfS3.toLocalIterator().next()}")

}

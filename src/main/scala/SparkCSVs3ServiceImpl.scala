import cats.effect.{Resource, Sync}
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

// fixme Caused by: com.amazonaws.SdkClientException: Unable to execute HTTP request: Certificate for <bucket.--NAME--.s3.amazonaws.com> doesn't match any of the subject alternative names:
//[*.s3.amazonaws.com, s3.amazonaws.com]

class SparkCSVs3ServiceImpl[F[_] : Sync](sparkSession: SparkSession,
                            inputS3Path: String, outputPath: String) extends SparkCSVService[F] {

  sparkSession.sparkContext
    .hadoopConfiguration.set("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"))
  sparkSession.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"))
  sparkSession.sparkContext
    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
  sparkSession.sparkContext.setLogLevel("ERROR")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

  override def process(): F[Unit] = Sync[F].pure {

    val file: RDD[String] = sparkSession.sparkContext.textFile(inputS3Path)

    val dataFrame = sparkSession.createDataFrame(file.map(Row(_)), StructType(List(StructField("text", StringType, false))))

    val contentFiltered = filterDataFrame(dataFrame)

    sparkSession.createDataFrame(contentFiltered).write.csv(outputPath)
  }

  override def close(): F[Unit] = Sync[F].pure(sparkSession.close())
}

object SparkCSVs3ServiceImpl {

  def make[F[_] : Sync](config: Config): Resource[F, SparkCSVService[F]] = {

    val inputPath = config.getString("input-s3-path")
    val outputPath = config.getString("output-path")

    Resource.make {
      val sparkSession: SparkSession = SparkSession.builder()
        .appName("Spark_CSV_Assignment")
        .config("spark.master", "local")
        .getOrCreate()
      Sync[F].pure(new SparkCSVs3ServiceImpl(sparkSession, inputPath, outputPath))
    } {
      service => service.close()
    }

  }
}

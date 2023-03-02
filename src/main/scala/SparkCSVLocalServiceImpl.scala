import cats.effect.{Resource, Sync}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkCSVLocalServiceImpl[F[_] : Sync](sparkSession: SparkSession,
                               inputPath: String, outputPath: String) extends SparkCSVService[F] {
  override def process(): F[Unit] = Sync[F].pure {
    val dataFrame: DataFrame = sparkSession.read
      .format("text")
      .option("InferSchema", "true")
      .load(inputPath)

    val contentFiltered = filterDataFrame(dataFrame)

    sparkSession.createDataFrame(contentFiltered).write.csv(outputPath)
  }

  override def close(): F[Unit] = Sync[F].pure(sparkSession.close())
}

object SparkCSVLocalServiceImpl {

  def make[F[_] : Sync](sparkSession: SparkSession, config: Config): Resource[F, SparkCSVService[F]] = {

    val inputPath = config.getString("input-path")
    val outputPath = config.getString("output-path")

    Resource.eval(Sync[F].pure(new SparkCSVLocalServiceImpl(sparkSession, inputPath, outputPath)))  }

}

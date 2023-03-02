import cats.effect.{IO, IOApp}
import com.typesafe.config.ConfigFactory

object Spark_CSV_App extends IOApp.Simple {

  override def run: IO[Unit] = {

    val config = ConfigFactory.load("application.conf")

    val appName = "Spark_CSV_Assignment"
    val (sparkConfKey, sparkConfVal) = ("spark.master", "local")

    (for {
      sparkSession <- SparkSessionResource.make[IO](appName,
                                                      (sparkConfKey, sparkConfVal))
      service      <- SparkCSVLocalServiceImpl.make[IO](sparkSession, config)
    } yield service).use {
      service => service.process()
    }

  }

}

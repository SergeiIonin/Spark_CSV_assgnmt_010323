import cats.effect.{Resource, Sync}
import org.apache.spark.sql.SparkSession

object SparkSessionResource {

  def make[F[_] : Sync](appName: String,
                        sparkConfig: (String, String)): Resource[F, SparkSession] = {
      Resource.make(Sync[F].defer {
        Sync[F].pure(SparkSession.builder()
          .appName(appName)
          .config(sparkConfig._1, sparkConfig._2)
          .getOrCreate())
      })(session => Sync[F].defer(Sync[F].pure(session.close())))
  }

}

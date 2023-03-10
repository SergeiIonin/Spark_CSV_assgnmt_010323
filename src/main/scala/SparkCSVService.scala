import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}

import scala.util.Try

trait SparkCSVService[F[_]] {

  def process(): F[Unit]

  protected def filterDataSet(dataSet: Dataset[String])(implicit enc1: Encoder[Option[(Int, Int)]],
                                                          enc2: Encoder[(Int, Int)],
                                                          enc3: Encoder[((Int, Int), Int)]): DataFrame = {
      val pat1 = "((\\d{1,19})*) ((\\d{1,19})*)".r
      val pat2 = "((\\d{1,19})*),((\\d{1,19})*)".r

      def parseString(s: String)(sep: String): (Int, Int) = {
        val temp = s.split(sep).toList
        (Try(temp.head.toInt).getOrElse(0), Try(temp.tail.head.toInt).getOrElse(0))
      }

      val tuplesRawDS = dataSet.map {
        case s@pat1(_*) => Some(parseString(s)(" "))
        case s@pat2(_*) => Some(parseString(s)(","))
        case _          => None
      }

      val tuplesDS = tuplesRawDS.filter(_.nonEmpty).map(_.get) // unfortunatelly there's no collect method like in Scala std

      tuplesDS
          .groupByKey(kv => kv)
          .mapGroups((tuple: (Int, Int), iterator: Iterator[(Int, Int)]) =>
            tuple -> iterator.toList.size)
          .filter(tupleToSize => tupleToSize._2 % 2 != 0)
          .map {
            case k -> _ => k._1 -> k._2
          }
          .withColumnRenamed("_1", "Key")
          .withColumnRenamed("_2", "Value")
    }

  def close(): F[Unit]

}

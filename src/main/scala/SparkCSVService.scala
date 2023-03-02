import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.jdk.CollectionConverters.IteratorHasAsScala

trait SparkCSVService[F[_]] {

  def process(): F[Unit]

  private def sanitizeBlanksWithZeros(tuple: (String, String)) = {
    val (k, v) = tuple
    if (k.nonEmpty && v.nonEmpty) tuple
    else if (k.isEmpty && v.nonEmpty) "0" -> v
    else if (k.nonEmpty && v.isEmpty) k -> "0"
    else "0" -> "0"
  }

  private def separateRowIntoKV(row: Row): (String, String) = {
    val string = row.getString(0)
    string.split(",").toList match {
      case k :: v :: Nil => sanitizeBlanksWithZeros(k -> v)
      case kv :: Nil =>
        val list = kv.split(" ").toList
        sanitizeBlanksWithZeros(list.head -> list.tail.head)
    }
  }

  protected def filterDataFrame(dataFrame: DataFrame): LazyList[(String, String)] = {
    val content = LazyList.from(LazyList.from(dataFrame.toLocalIterator().asScala).map(separateRowIntoKV)
      .groupMapReduce(_._1) {
        case _ -> v => v -> 1
      } {
        case (k1 -> v1, _ -> v2) => (k1, v1 + v2)
      })
    content.collect {
      case (key, (value, occurence)) if occurence % 2 != 0 => s"$key" -> s"$value"
    }
  }

  def close(): F[Unit]

}

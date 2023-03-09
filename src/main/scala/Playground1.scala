import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Playground1 extends App {
  val sparkSession = SparkSession.builder()
    .appName("Complex_Types")
    .config("spark.master", "local")
    .getOrCreate()

  val schema =
    StructType {
      Array(
        StructField("key", IntegerType),
        StructField("value", IntegerType)
      )
    }

  val tuplesDF = sparkSession
    .read
    .format("csv")
    .option("header", "true")
    .option("nullValue", "0")
    .schema(schema)
    .csv("src/main/resources/data/testFile-0")

  import sparkSession.implicits._

  val tuplesDS = tuplesDF.as[(Int, Int)]

  val tuplesGrouped = tuplesDS.groupByKey {
    case k -> v => k
  }.reduceGroups {
    case (_ ->  v1, _ ->  v2) => v2 -> (v1 + v2)
  }.map {
    case k -> (v -> sum) =>
      val (value, amnt) = if (k == v) (sum, 1) else (v, sum / v)
      k -> (value -> amnt)
  }//.select(col("_2"))
  /*filter {
    case k -> (v -> amnt) => if (amnt % 2) k -> v
  }*/

  tuplesGrouped.where(col("_2").getItem("_2") % 2 =!= 0).show()

  //tuplesGrouped.select(col("_2")).printSchema()

  tuplesGrouped.show()

}

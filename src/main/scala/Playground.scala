import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}

object Playground extends App {

  val sparkSession = SparkSession.builder()
    .appName("Complex_Types")
    .config("spark.master", "local")
    .getOrCreate()

  val stocksDataPat = "MMM dd yyyy"

  val stockSchema =
    StructType {
      Array(
        StructField("symbol", StringType),
        StructField("date", StringType), // DateType
        StructField("price", DoubleType)
      )
    }

  val stocksDF = sparkSession
    .read
    .format("csv")
    .option("dateFormat", stocksDataPat)
    .option("header", "true")
    .option("nullValue", "")
    .schema(stockSchema)
    .csv("src/main/resources/data/stocks.csv")

  import sparkSession.implicits._

  final case class Stock(symbol: String, date: String, price: Double)

  val stocksDS = stocksDF.as[Stock]

  val stocksGrouped = stocksDS.groupByKey {
    case stock@Stock(s, _, _) => s -> stock
  }.reduceGroups {
    case (s1, s2) => if (s1.price > s2.price) s1 else s2
  }


}

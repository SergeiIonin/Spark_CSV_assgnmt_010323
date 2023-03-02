import SparkCSVServiceSpec.{clearAllFiles, clearDirAndAllFiles, expectedOutput}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.config.ConfigFactory
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.{Files, Path}
import java.util
import java.util.stream.{Collector, Collectors}
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.{IterableHasAsJava, ListHasAsScala}

class SparkCSVServiceSpec extends AnyFlatSpec with Matchers {

  "SparkCSVLocalServiceImpl" should "properly read all files and write the aggregate" in {
    val config = ConfigFactory.load("application.conf")
    val inputPath = config.getString("input-path") // "src/test/resources/data/testFiles/csv"
    val outputPath = config.getString("output-path") // "src/test/resources/data/output/csv"

    clearAllFiles(inputPath)

    SparkCSVServiceSpec.generateFiles(inputPath)

    (for {
      sparkSession <- SparkSessionResource.make[IO]("Spark_CSV_Test",
                                                    ("spark.master", "local"))
      service <- SparkCSVLocalServiceImpl.make[IO](sparkSession, config)
    } yield service).use {
      service => service.process()
    }.unsafeRunSync()

    val outputFile = {
      val collector: Collector[Path, _, util.List[Path]] = Collectors.toList()
      val files: List[Path] = Files.list(Path.of(outputPath)).collect(collector).asScala.toList
      files.filter(p => p.toFile.getName.endsWith(".csv")).head
    }

    val fileOutContent = Files.readAllLines(outputFile).asScala.toList

    fileOutContent.length shouldBe expectedOutput.length
    println("----" * 10)
    println(expectedOutput)
    println("----" * 10)
    println(fileOutContent)
    println("----" * 10)
    fileOutContent.sameElements(expectedOutput) shouldBe true
  }

}

object SparkCSVServiceSpec {

  def clearAllFiles(uri: String): Unit = {
    if (Files.exists(Path.of(uri))) {
      val collector: Collector[Path, _, util.List[Path]] = Collectors.toList()
      val paths: List[Path] = Files.list(Path.of(uri)).collect(collector).asScala.toList
      paths.foreach(Files.deleteIfExists)
    } else ()
  }

  val totalAmountOfRecords = 45
  val totalAmountOfFiles = 5
  val sliceSize = totalAmountOfRecords / totalAmountOfFiles

  val keyGen = Gen.oneOf((0 to 100).toList)
  val valueGen = Gen.oneOf((0 to 100).toList)

  val occurrenceGen = Gen.oneOf(List(1, 2, 3, 4, 5, 6, 7))

  val fileNameGen = Gen.alphaNumStr

  final case class Record(key: String, value: String, occurrence: Int)

  def recordGen(occurrence: Int) = {
    for {
      key       <- keyGen
      value     <- valueGen
    } yield Record(s"$key", s"$value", occurrence)
  }

  def getRecords(): List[Record] = {
    @tailrec def iter(acc: List[Record]): List[Record] = {
      if (acc.size < totalAmountOfRecords) {
        val rec = (for {
          occurrence <- occurrenceGen
          record <- recordGen(occurrence)
        } yield record).sample.get
        iter(rec :: acc)
      } else acc
    }
    iter(List())
  }

  def getListTuples(): List[(String, String)] =
    getRecords().flatMap {
      case Record(k, v, o) => List.fill(o)(k -> v)
    }.take(totalAmountOfRecords)

  def getListTuplesWithOddoccurrences(listTuples: List[(String, String)]) = {
    listTuples.groupMapReduce(_._1)(tuple => (tuple._2, 1))((first, second) => (first._1, first._2 + second._2)).toList.collect {
      case key -> (value -> occurrence) if occurrence % 2 != 0 => s"$key,$value"
    }
  }

  val listTuples: List[(String, String)] = getListTuples()
  val indexToSlice: List[(Int, List[(String, String)])] = (0 until totalAmountOfFiles).toList
    .map(index => index -> listTuples.slice(index * sliceSize, index * sliceSize + sliceSize))

  def generateFiles(path: String) = {
    indexToSlice.map {
      case (index, slice) =>
        val uri = s"$path/testFile-$index"
        val content = slice.map {
          case (k, v) => s"$k,$v"
        }
        Files.write(Path.of(uri), content.asJava)
    }
    ()
  }

  val expectedOutput: List[String] = getListTuplesWithOddoccurrences(listTuples)

}
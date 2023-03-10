ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

val sparkVersion = "3.3.2"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

lazy val root = (project in file("."))
  .settings(
    name := "CSV_Spark_Test",
    libraryDependencies := Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.hadoop" % "hadoop-common" % "3.3.4",
      "org.apache.hadoop" % "hadoop-client" % "3.3.4",
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4",

      "org.typelevel" %% "cats-effect" % "3.4.4",
      "org.scalatest" %% "scalatest" % "3.2.10" % Test,
      "org.scalacheck" %% "scalacheck" % "1.15.4" % Test,
      "com.typesafe" % "config" % "1.4.2",
      // logging
      "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
))

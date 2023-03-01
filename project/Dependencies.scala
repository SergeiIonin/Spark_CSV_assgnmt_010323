import sbt._

object Dependencies {

  private object Spark {
    private val sparkVersion = "3.3.2"

    private val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
    private val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

    val dependencies = List(sparkCore, sparkSql)
  }

  private object AWS {
    private val awsJavaSdkVersion = "2.19.8"

    private val awsJavaSdk = "software.amazon.awssdk" % "s3" % awsJavaSdkVersion

    val dependencies = List(awsJavaSdk)
  }

  private object Misc {
    private val catsEffectVersion = "3.4.4"
    private val wvletVersion = "23.2.2"
    private val slf4jjdk14Version = "2.0.6"

    private val catsEffect = "org.typelevel" %% "cats-effect" % catsEffectVersion

    //Logs
    private val wvlet = "org.wvlet.airframe" %% "airframe-log" % wvletVersion
    private val slf4jjdk14 = "org.slf4j" % "slf4j-jdk14" % slf4jjdk14Version

    val dependencies = List(catsEffect, wvlet, slf4jjdk14)
  }

  private object Testing {
    private val scalaTest = "org.scalatest" %% "scalatest" % "3.2.10"
    private val scalaTestPlusCheck = "org.scalatestplus" %% "scalacheck-1-15" % "3.2.10.0"
    private val scalacheck = "org.scalacheck" %% "scalacheck" % "1.15.4"
    private val scalacheckCats = "io.chrisdavenport" %% "cats-scalacheck" % "0.3.1"
    private val scalacheckEffect = "org.typelevel" %% "scalacheck-effect" % "1.0.3"
    private val scalaTestCatEffect = "org.typelevel" %% "cats-effect-testing-scalatest" % "1.4.0"
    private val scalaMock = "org.scalamock" %% "scalamock" % "5.2.0"

    val dependencies =
      Seq(scalaTest, scalacheck, scalaTestPlusCheck, scalacheckCats,
        scalacheckEffect, scalaTestCatEffect, scalaMock).map(_ % "test")
  }

  val dependenciesAll: List[ModuleID] = Spark.dependencies ++
                          Misc.dependencies ++ Testing.dependencies ++ AWS.dependencies

}

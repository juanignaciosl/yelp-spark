import sbt._

object Dependencies {
  lazy val spark = "org.apache.spark" %% "spark-core" % "2.4.3"
  lazy val sparkSQL = "org.apache.spark" %% "spark-sql" % "2.4.3"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.6"
}

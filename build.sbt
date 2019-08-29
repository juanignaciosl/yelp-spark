import Dependencies._

ThisBuild / scalaVersion := "2.12.8"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.juanignaciosl"
ThisBuild / organizationName := "juanignaciosl"

lazy val root = (project in file("."))
  .settings(
    name := "Yelp Spark",
    libraryDependencies ++= Seq(
      spark, sparkSQL, scalaTest % Test
    )
  )

fork in run := true

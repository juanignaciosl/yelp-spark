import Dependencies._

ThisBuild / scalaVersion := "2.11.8"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.juanignaciosl"
ThisBuild / organizationName := "juanignaciosl"

lazy val root = (project in file("."))
  .settings(
    name := "YelpSpark",
    libraryDependencies ++= Seq(
      spark, sparkSQL, scalaTest % Test
    ),
    mainClass in (Compile, run) := Some("com.juanignaciosl.yelp.YelpBusinessRunner")
  )

fork in run := true

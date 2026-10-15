import Dependencies._

ThisBuild / scalaVersion     := "3.9.0"
ThisBuild / version          := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-in-practice",
    libraryDependencies ++= Seq(kafkaClients, jacksonDatabind, munit % Test)
  )

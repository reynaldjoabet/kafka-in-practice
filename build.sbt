import Dependencies._

ThisBuild / scalaVersion := "3.9.0"
ThisBuild / version      := "0.1.0-SNAPSHOT"

ThisBuild / scalacOptions := Seq(
  "-encoding",
  "UTF-8",
  "-no-indent",
  "-deprecation",
  "-feature",
  "-unchecked",
  // "-Werror",
  // "-Wunused:all",
  "-Wvalue-discard",
  "-Wnonunit-statement",
  "-language:strictEquality",
  "-Xcheck-macros",
  "-Xmax-inlines:64"
)

lazy val root = (project in file("."))
  .settings(
    name                 := "kafka-in-practice",
    libraryDependencies ++= Seq(
      kafkaClients,
      jacksonDatabind,
      fs2Kafka,
      log4cats,
      logbackClassic,
      munit                    % Test,
      munitCatsEffect          % Test,
      kafkaTestKit             % Test,
      kafkaClientsTestFixtures % Test,
      bouncyCastle             % Test
    ),
    // The test broker runs inside the test JVM; fork so it gets its own
    // heap and is torn down with the forked JVM.
    Test / fork := true
  )

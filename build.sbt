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
    libraryDependencies ++=
      Dependencies.kafka ++
        Dependencies.json ++
        Dependencies.logging ++
        Dependencies.testing ++
        Dependencies.kafkaTestKit,
    // The test broker runs inside the test JVM; fork so it gets its own
    // heap and is torn down with the forked JVM.
    Test / fork := true
  )

semanticdbEnabled := true

lazy val sbtForkSettings = Seq(
  fork                := true,
  run / baseDirectory := (ThisBuild / baseDirectory).value,
  run / envVars       := sys.env,
  run / javaOptions  ++= sys.props.collect {
    case (key, value) if key.startsWith("env.") => s"-D$key=$value"
  }.toSeq
)

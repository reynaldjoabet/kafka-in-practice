import sbt._

object Dependencies {
  lazy val munit = "org.scalameta" %% "munit" % "0.7.29"

  lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % "4.4.0"
  lazy val jacksonDatabind = "tools.jackson.core" % "jackson-databind" % "3.2.3"
}

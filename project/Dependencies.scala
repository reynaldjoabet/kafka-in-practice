import sbt._

object Dependencies {

  lazy val kafkaVersion = "4.4.0"

  lazy val kafkaClients    = "org.apache.kafka"   % "kafka-clients"    % kafkaVersion
  lazy val jacksonDatabind = "tools.jackson.core" % "jackson-databind" % "3.2.3"
  lazy val fs2Kafka        = "org.typelevel"     %% "fs2-kafka"        % "4.1.3"
  lazy val log4cats        = "org.typelevel"     %% "log4cats-slf4j"   % "2.8.0"
  lazy val logbackClassic  = "ch.qos.logback"     % "logback-classic"  % "1.6.5"

  lazy val munit           = "org.scalameta" %% "munit"             % "1.3.6"
  lazy val munitCatsEffect = "org.typelevel" %% "munit-cats-effect" % "2.2.1"
  // An in-JVM KRaft cluster (the one Kafka's own integration tests use), so
  // the client tests run against a real broker without Docker.
  lazy val kafkaTestKit = "org.apache.kafka" % "kafka-test-common-runtime" % kafkaVersion
  // The test kit uses helpers (org.apache.kafka.test.TestUtils) from this jar.
  lazy val kafkaClientsTestFixtures = kafkaClients.classifier("test-fixtures")
  // ...and those helpers need Bouncy Castle (same version as Kafka's build).
  lazy val bouncyCastle = "org.bouncycastle" % "bcpkix-jdk18on" % "1.86"

}

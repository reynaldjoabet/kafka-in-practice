import sbt._

object Dependencies {

  private object Versions {

    val kafka           = "4.4.0"
    val fs2Kafka        = "4.1.3"
    val jackson         = "3.2.3"
    val log4cats        = "2.8.0"
    val logback         = "1.6.5"
    val munit           = "1.3.6"
    val munitCatsEffect = "2.2.1"
    // Same version as Kafka's own build.
    val bouncyCastle = "1.86"

  }

  val kafka = Seq(
    "org.apache.kafka" % "kafka-clients" % Versions.kafka,
    "org.typelevel"   %% "fs2-kafka"     % Versions.fs2Kafka
  )

  val json = Seq(
    "tools.jackson.core" % "jackson-databind" % Versions.jackson
  )

  val logging = Seq(
    "org.typelevel" %% "log4cats-slf4j"  % Versions.log4cats,
    "ch.qos.logback" % "logback-classic" % Versions.logback
  )

  val testing = Seq(
    "org.scalameta" %% "munit"             % Versions.munit           % Test,
    "org.typelevel" %% "munit-cats-effect" % Versions.munitCatsEffect % Test
  )

  // An in-JVM KRaft cluster (the one Kafka's own integration tests use), so the client tests
  // run against a real broker without Docker. It needs helpers from the kafka-clients test
  // fixtures (org.apache.kafka.test.TestUtils), which in turn need Bouncy Castle.
  val kafkaTestKit = Seq(
    "org.apache.kafka"  % "kafka-test-common-runtime" % Versions.kafka        % Test,
    ("org.apache.kafka" % "kafka-clients"             % Versions.kafka        % Test).classifier("test-fixtures"),
    "org.bouncycastle"  % "bcpkix-jdk18on"            % Versions.bouncyCastle % Test
  )

}

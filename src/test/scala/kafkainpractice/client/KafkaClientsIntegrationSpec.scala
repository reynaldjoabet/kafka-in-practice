package kafkainpractice.client

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.effect.kernel.Ref
import cats.syntax.all.*
import fs2.kafka.*
import fs2.Chunk

import munit.CatsEffectSuite
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.common.test.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.common.TopicPartition

/**
  * Runs the producer and consumer against a real single-node KRaft cluster in this JVM.
  */
class KafkaClientsIntegrationSpec extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 3.minutes

  private val cluster = ResourceSuiteLocalFixture(
    "kafka",
    Resource.make(IO.blocking(startCluster()))(kit => IO.blocking(kit.close()))
  )

  // cats-effect treats LinkageError (e.g. a class missing from the test classpath) as fatal and
  // shuts its runtime down, which leaves munit waiting forever. Rethrow it as a plain failure.
  private def startCluster(): KafkaClusterTestKit =
    try {
      val nodes = new TestKitNodes.Builder()
        .setCombined(true)
        .setNumBrokerNodes(1)
        .setNumControllerNodes(1)
        .build()
      val kit = new KafkaClusterTestKit.Builder(nodes)
        // One broker, so the internal topics can only have one replica.
        .setConfigProp("offsets.topic.replication.factor", "1")
        .setConfigProp("offsets.topic.num.partitions", "1")
        .setConfigProp("transaction.state.log.replication.factor", "1")
        .setConfigProp("transaction.state.log.min.isr", "1")
        .setConfigProp("group.initial.rebalance.delay.ms", "0")
        .build()
      kit.format()
      kit.startup()
      kit.waitForReadyBrokers()
      kit
    } catch {
      case e: LinkageError => throw new RuntimeException("Could not start the test cluster", e)
    }

  override def munitFixtures = List(cluster)

  private val names                  = new AtomicInteger
  private def unique(prefix: String) = s"$prefix-${names.incrementAndGet()}"

  private def connection(clientId: String) =
    KafkaConnection(cluster().bootstrapServers(), clientId)

  private def admin: Resource[IO, Admin] =
    Resource.fromAutoCloseable(IO.blocking(cluster().admin()))

  private def createTopic(name: String, partitions: Int): IO[Unit] =
    admin.use(a =>
      IO.blocking(
        a.createTopics(List(new NewTopic(name, partitions, 1.toShort)).asJava).all().get()
      ).void
    )

  private def committedOffsets(group: String): IO[Map[TopicPartition, Long]] =
    admin.use { a =>
      IO.blocking(a.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get())
        .map(_.asScala.collect { case (tp, om) if om != null => tp -> om.offset }.toMap)
    }

  private def produce(topic: String, records: List[(String, String)]): IO[Unit] =
    EventProducer
      .resource(
        ProducerConfig(connection(unique("producer"))),
        Serializer.string[IO],
        Serializer.string[IO]
      )
      .use(_.sendAll(Chunk.from(records.map((k, v) => ProducerRecord(topic, k, v)))).void)

  private def consumerConfig(topic: String, group: String, dlq: Option[String] = None) =
    ConsumerConfig(
      connection(unique("consumer")),
      groupId = group,
      topics = NonEmptyList.one(topic),
      commitWithin = 200.millis,
      retry = RetryPolicy(maxRetries = 2, initialBackoff = 5.millis, maxBackoff = 20.millis),
      deadLetterTopic = dlq
    )

  /**
    * Runs the consumer in the background until `done` holds, then stops it gracefully.
    */
  private def consumeUntil[V](
      config: ConsumerConfig,
      valueDeserializer: ValueDeserializer[IO, V],
      done: IO[Boolean]
  )(handler: ConsumerRecord[String, V] => IO[Unit]): IO[Unit] =
    EventConsumer.resource(config, Deserializer.string[IO], valueDeserializer).use { consumer =>
      consumer.run(handler).background.use { outcome =>
        val finishedEarly =
          outcome.flatMap(o => IO.raiseError(new AssertionError(s"consumer stopped: $o")))
        IO.race(finishedEarly, waitFor(done)).void
      }
    }

  private def waitFor(condition: IO[Boolean]): IO[Unit] =
    (IO.sleep(100.millis) >> condition).iterateUntil(identity).void.timeout(90.seconds)

  private def readAll(topic: String, count: Int): IO[List[ConsumerRecord[String, String]]] =
    KafkaConsumer
      .stream(
        ConsumerSettings(Deserializer.string[IO], Deserializer.string[IO])
          .withBootstrapServers(cluster().bootstrapServers())
          .withGroupId(unique("reader"))
          .withAutoOffsetReset(AutoOffsetReset.Earliest)
      )
      .subscribeTo(topic)
      .records
      .map(_.record)
      .take(count.toLong)
      .compile
      .toList
      .timeout(90.seconds)

  private def header(r: ConsumerRecord[?, ?], key: String): Option[String] =
    r.headers.withKey(key).map(h => new String(h.value, UTF_8))

  test("delivers every record, in order per key, and commits through the end of each partition") {
    val topic   = unique("orders")
    val group   = unique("billing")
    val records = (1 to 300).toList.map(i => s"customer-${i % 7}" -> i.toString)
    for {
      _    <- createTopic(topic, partitions = 3)
      _    <- produce(topic, records)
      seen <- Ref[IO].of(Vector.empty[(String, String)])
      _    <- consumeUntil(
             consumerConfig(topic, group),
             Deserializer.string[IO],
             seen.get.map(_.size >= records.size)
           ) { r =>
             seen.update(_ :+ (r.key -> r.value))
           }
      handled   <- seen.get
      committed <- committedOffsets(group)
    } yield {
      assertEquals(handled.size, records.size, "each record handled exactly once in this run")
      assertEquals(
        handled.toList.groupMap(_._1)(_._2),
        records.groupMap(_._1)(_._2),
        "order per key"
      )
      assertEquals(
        committed.values.sum,
        records.size.toLong,
        "committed offsets cover every record"
      )
    }
  }

  test("undecodable records and records the handler gives up on go to the dead-letter topic") {
    val topic = unique("payments")
    val dlq   = s"$topic.dlq"
    val group = unique("ledger")
    // "bad" cannot be decoded; the handler always fails on "fail" and fails "flaky" twice.
    val values = Deserializer.string[IO].flatMap {
      case "bad" => Deserializer.failWith[IO, String]("not a payment")
      case _     => Deserializer.string[IO]
    }
    for {
      _ <- createTopic(topic, partitions = 1)
      _ <- produce(
             topic,
             List("a" -> "ok-1", "b" -> "bad", "c" -> "fail", "d" -> "flaky", "e" -> "ok-2")
           )
      attempts <- Ref[IO].of(Map.empty[String, Int])
      done     <- Ref[IO].of(Set.empty[String])
      _        <- consumeUntil(
             consumerConfig(topic, group, Some(dlq)),
             values,
             done.get.map(_ == Set("ok-1", "flaky", "ok-2"))
           ) { r =>
             attempts
               .updateAndGet(m => m.updated(r.value, m.getOrElse(r.value, 0) + 1))
               .map(_(r.value))
               .flatMap {
                 case _ if r.value == "fail" =>
                   IO.raiseError(new RuntimeException("downstream unavailable"))
                 case n if r.value == "flaky" && n < 3 =>
                   IO.raiseError(new RuntimeException("timeout"))
                 case _ => done.update(_ + r.value)
               }
           }
      tries       <- attempts.get
      deadLetters <- readAll(dlq, 2)
      _           <- waitFor(committedOffsets(group).map(_.values.sum == 5L))
    } yield {
      assertEquals(
        tries,
        Map("ok-1" -> 1, "fail" -> 3, "flaky" -> 3, "ok-2" -> 1),
        "attempts per value"
      )
      assertEquals(
        deadLetters.map(r => (r.key, r.value)),
        List("b" -> "bad", "c" -> "fail"),
        "original key and value kept"
      )
      val List(decodeFailure, handlerFailure) = deadLetters: @unchecked
      assertEquals(header(decodeFailure, DeadLetterHeaders.Stage), Some("Decode"))
      assertEquals(header(decodeFailure, DeadLetterHeaders.Attempts), Some("1"))
      assertEquals(header(handlerFailure, DeadLetterHeaders.Stage), Some("Handler"))
      assertEquals(header(handlerFailure, DeadLetterHeaders.Attempts), Some("3"))
      assertEquals(header(handlerFailure, DeadLetterHeaders.Offset), Some("2"))
      assertEquals(
        header(handlerFailure, DeadLetterHeaders.ExceptionMsg),
        Some("downstream unavailable")
      )
    }
  }

  test("without a dead-letter topic a failing record stops the consumer and is not committed") {
    val topic = unique("shipments")
    val group = unique("warehouse")
    for {
      _       <- createTopic(topic, partitions = 1)
      _       <- produce(topic, List("a" -> "ok-1", "b" -> "ok-2", "c" -> "poison", "d" -> "ok-3"))
      handled <- Ref[IO].of(Vector.empty[String])
      result  <-
        EventConsumer
          .resource(consumerConfig(topic, group), Deserializer.string[IO], Deserializer.string[IO])
          .use(_.run { r =>
            if r.value == "poison" then IO.raiseError(new RuntimeException("cannot ship"))
            else handled.update(_ :+ r.value)
          })
          .attempt
          .timeout(90.seconds)
      seen      <- handled.get
      committed <- committedOffsets(group)
    } yield {
      result match {
        case Left(e: RecordProcessingException) =>
          assertEquals((e.offset, e.attempts), (2L, 3))
        case other => fail(s"expected RecordProcessingException, got $other")
      }
      assertEquals(seen, Vector("ok-1", "ok-2"), "nothing after the poison record was handled")
      assert(committed.values.forall(_ <= 2L), s"committed past the failing record: $committed")
    }
  }

}

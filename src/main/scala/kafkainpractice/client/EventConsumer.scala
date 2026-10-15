package kafkainpractice.client

import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.duration.*

import cats.data.NonEmptyList
import cats.effect.{Async, Fiber, Outcome, Resource}
import cats.effect.std.Random
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.Parallel
import fs2.kafka.*

import org.apache.kafka.clients.consumer.{
  ConsumerConfig as KafkaConsumerConfig,
  CooperativeStickyAssignor
}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

/**
  * Consumer tuning.
  *
  * @param groupId
  *   consumers with the same group id split the topics' partitions between them
  * @param groupInstanceId
  *   a stable, unique id per instance (e.g. the pod name in a StatefulSet) enables static
  *   membership (KIP-345): a restart within `sessionTimeout` gets its partitions back without a
  *   rebalance
  * @param autoOffsetReset
  *   where a group with no committed offset starts. fs2-kafka's default is `None`, which fails on a
  *   new group, so this defaults to the beginning of the log
  * @param readCommitted
  *   skip records from aborted transactions and wait for open transactions to finish. Has no cost
  *   on topics without transactional producers
  * @param maxPollRecords
  *   records returned per poll, and so the size of the chunks the streams see
  * @param maxPollInterval
  *   fs2-kafka polls on its own fiber regardless of processing speed, so slow processing does not
  *   hit this limit; it only guards against a stuck polling fiber
  * @param sessionTimeout
  *   how long the group coordinator waits for a heartbeat before it removes this member
  * @param commitEvery
  *   commit after this many processed records, or after `commitWithin`, whichever comes first
  * @param retry
  *   retries for a failing handler, per record
  * @param deadLetterTopic
  *   where records go when they cannot be decoded or the handler gives up on them. Without one,
  *   such a record stops the consumer, so it is never skipped silently
  * @param shutdownTimeout
  *   how long a graceful shutdown waits for in-flight records before it gives up
  */
final case class ConsumerConfig(
    connection: KafkaConnection,
    groupId: String,
    topics: NonEmptyList[String],
    groupInstanceId: Option[String] = None,
    autoOffsetReset: AutoOffsetReset = AutoOffsetReset.Earliest,
    readCommitted: Boolean = true,
    maxPollRecords: Int = 500,
    maxPollInterval: FiniteDuration = 5.minutes,
    sessionTimeout: FiniteDuration = 45.seconds,
    commitEvery: Int = 500,
    commitWithin: FiniteDuration = 5.seconds,
    retry: RetryPolicy = RetryPolicy(),
    deadLetterTopic: Option[String] = None,
    shutdownTimeout: FiniteDuration = 30.seconds,
    extraProperties: Map[String, String] = Map.empty
)

/**
  * Raised when a record cannot be processed and there is no dead-letter topic to send it to.
  */
final class RecordProcessingException(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val attempts: Int,
    cause: Throwable
) extends RuntimeException(
      s"Giving up on $topic-$partition@$offset after $attempts attempt(s): ${cause.getMessage}",
      cause
    )

/**
  * Headers added to a dead-lettered record. The original key, value and headers are kept byte for
  * byte, so the record can be inspected or replayed to the source topic.
  */
object DeadLetterHeaders {

  val Topic          = "dlq.original.topic"
  val Partition      = "dlq.original.partition"
  val Offset         = "dlq.original.offset"
  val Timestamp      = "dlq.original.timestamp"
  val ConsumerGroup  = "dlq.consumer.group"
  val Stage          = "dlq.failure.stage"
  val Attempts       = "dlq.failure.attempts"
  val ExceptionClass = "dlq.exception.class"
  val ExceptionMsg   = "dlq.exception.message"

}

/**
  * At-least-once consumer.
  *
  *   - Each assigned partition is processed by its own stream, so records of one partition are
  *     handled in order, one at a time, while partitions run in parallel.
  *   - An offset is committed only after its record was handled or dead-lettered. Commits are
  *     batched (`commitEvery` / `commitWithin`).
  *   - The consumer reads raw bytes and decodes them itself, so a record that cannot be decoded
  *     reaches the dead-letter topic unchanged instead of failing the stream.
  *   - On cancellation (e.g. SIGTERM through `IOApp`), it stops fetching, finishes the records
  *     already fetched, commits their offsets and only then leaves the group.
  *
  * At-least-once means a record can be handled more than once: after a crash, or after a rebalance
  * that moves a partition before its last offsets were committed. Make the handler idempotent, for
  * example by keying writes on topic, partition and offset or on a business id.
  */
final class EventConsumer[F[_]: Async: Parallel, K, V] private (
    config: ConsumerConfig,
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V],
    deadLetters: Option[KafkaProducer[F, Array[Byte], Array[Byte]]],
    logger: Logger[F]
)(using Random[F]) {

  import EventConsumer.*

  /**
    * Consumes until cancelled or until a record fails without a dead-letter topic.
    */
  def run(handler: ConsumerRecord[K, V] => F[Unit]): F[Unit] =
    KafkaConsumer.resource(EventConsumer.settings[F](config)).use { consumer =>
      Async[F].uncancelable { poll =>
        consume(consumer, handler).start.flatMap { fiber =>
          poll(fiber.joinWithNever).onCancel(stopGracefully(consumer, fiber))
        }
      }
    }

  private def consume(
      consumer: KafkaConsumer[F, Array[Byte], Array[Byte]],
      handler: ConsumerRecord[K, V] => F[Unit]
  ): F[Unit] =
    consumer.subscribe(config.topics) >>
      consumer.partitionedRecords
        .map(_.evalMap(committable => process(committable.record, handler).as(committable.offset)))
        .parJoinUnbounded
        .through(commitBatchWithin(config.commitEvery, config.commitWithin))
        .compile
        .drain

  // stopConsuming ends the streams once the records already fetched are processed;
  // commitBatchWithin then commits what is left before the consumer is closed.
  private def stopGracefully(
      consumer: KafkaConsumer[F, Array[Byte], Array[Byte]],
      fiber: Fiber[F, Throwable, Unit]
  ): F[Unit] =
    logger.info(s"Stopping consumer for group ${config.groupId}") >>
      consumer.stopConsuming >>
      fiber.join
        .timeoutTo(
          config.shutdownTimeout,
          logger.warn(
            s"In-flight records not finished within ${config.shutdownTimeout}; " +
              "they will be redelivered"
          ) >> fiber.cancel.as(Outcome.canceled[F, Throwable, Unit])
        )
        .flatMap {
          case Outcome.Errored(e) => logger.error(e)("Consumer failed while stopping")
          case _                  => logger.info(s"Consumer for group ${config.groupId} stopped")
        }

  private def process(
      raw: ConsumerRecord[Array[Byte], Array[Byte]],
      handler: ConsumerRecord[K, V] => F[Unit]
  ): F[Unit] =
    decode(raw).attempt.flatMap {
      case Left(e) =>
        // Decoding is deterministic, so retrying cannot help.
        deadLetter(raw, Stage.Decode, e, attempts = 1)
      case Right(record) =>
        config.retry
          .retry(
            handler(record),
            retryable = isRetryable,
            onRetry = (attempt, e) =>
              logger.warn(e)(
                s"Handler failed for ${position(raw)}, retry $attempt of ${config.retry.maxRetries}"
              )
          )
          .flatMap {
            case Right(())           => Async[F].unit
            case Left((e, attempts)) => deadLetter(raw, Stage.Handler, e, attempts)
          }
    }

  private def decode(raw: ConsumerRecord[Array[Byte], Array[Byte]]): F[ConsumerRecord[K, V]] =
    raw.bitraverse(
      keyDeserializer.deserialize(raw.topic, raw.headers, _),
      valueDeserializer.deserialize(raw.topic, raw.headers, _)
    )

  private def deadLetter(
      raw: ConsumerRecord[Array[Byte], Array[Byte]],
      stage: Stage,
      error: Throwable,
      attempts: Int
  ): F[Unit] =
    (deadLetters, config.deadLetterTopic).tupled match {
      case Some((producer, topic)) =>
        val record = ProducerRecord(topic, raw.key, raw.value)
          .withHeaders(raw.headers.concat(deadLetterHeaders(raw, stage, error, attempts)))
        logger.error(error)(
          s"Sending ${position(raw)} to $topic after $attempts attempt(s) ($stage)"
        ) >>
          // Wait for the acknowledgement: the source offset is committed right after this.
          producer.produce(ProducerRecords.one(record)).flatten.void
      case None =>
        Async[F].raiseError(
          RecordProcessingException(raw.topic, raw.partition, raw.offset, attempts, error)
        )
    }

  private def deadLetterHeaders(
      raw: ConsumerRecord[Array[Byte], Array[Byte]],
      stage: Stage,
      error: Throwable,
      attempts: Int
  ): Headers = {
    def h(key: String, value: String) = Header(key, value.getBytes(UTF_8))
    Headers.fromSeq(
      List(
        h(DeadLetterHeaders.Topic, raw.topic),
        h(DeadLetterHeaders.Partition, raw.partition.toString),
        h(DeadLetterHeaders.Offset, raw.offset.toString),
        h(DeadLetterHeaders.ConsumerGroup, config.groupId),
        h(DeadLetterHeaders.Stage, stage.toString),
        h(DeadLetterHeaders.Attempts, attempts.toString),
        h(DeadLetterHeaders.ExceptionClass, error.getClass.getName),
        h(DeadLetterHeaders.ExceptionMsg, Option(error.getMessage).getOrElse(""))
      ) ++ raw.timestamp.toOption.map(ts => h(DeadLetterHeaders.Timestamp, ts.toString))
    )
  }

}

object EventConsumer {

  private enum Stage {
    case Decode, Handler
  }

  private def position(r: ConsumerRecord[?, ?]): String = s"${r.topic}-${r.partition}@${r.offset}"

  /**
    * Errors that retrying the same record will not fix. Everything else is retried.
    */
  private val isRetryable: Throwable => Boolean = {
    case _: IllegalArgumentException | _: DeserializationException => false
    case _                                                         => true
  }

  /**
    * Builds the consumer. With a dead-letter topic it also opens a producer, which `producer` can
    * share with the rest of the application.
    */
  def resource[F[_]: Async: Parallel, K, V](
      config: ConsumerConfig,
      keyDeserializer: KeyDeserializer[F, K],
      valueDeserializer: ValueDeserializer[F, V],
      producer: Option[ProducerConfig] = None
  ): Resource[F, EventConsumer[F, K, V]] = {
    val deadLetterProducer = config.deadLetterTopic.traverse { _ =>
      val producerConfig = producer.getOrElse(
        ProducerConfig(config.connection.copy(clientId = s"${config.connection.clientId}-dlq"))
      )
      KafkaProducer.resource(
        EventProducer.settings(producerConfig, Serializer.identity[F], Serializer.identity[F])
      )
    }
    for {
      dlq    <- deadLetterProducer
      random <- Resource.eval(Random.scalaUtilRandom[F])
      logger <- Resource.eval(Slf4jLogger.fromClass[F](classOf[EventConsumer[F, K, V]]))
    } yield {
      given Random[F] = random
      new EventConsumer(config, keyDeserializer, valueDeserializer, dlq, logger)
    }
  }

  def settings[F[_]: Async](
      config: ConsumerConfig
  ): ConsumerSettings[F, Array[Byte], Array[Byte]] =
    ConsumerSettings(Deserializer.identity[F], Deserializer.identity[F])
      .withBootstrapServers(config.connection.bootstrapServers)
      .withClientId(config.connection.clientId)
      .withProperties(config.connection.properties)
      .withGroupId(config.groupId)
      .pipe(s => config.groupInstanceId.fold(s)(s.withGroupInstanceId))
      .withAutoOffsetReset(config.autoOffsetReset)
      .withIsolationLevel(
        if config.readCommitted then IsolationLevel.ReadCommitted
        else IsolationLevel.ReadUncommitted
      )
      .withEnableAutoCommit(false)
      .withMaxPollRecords(config.maxPollRecords)
      .withMaxPollInterval(config.maxPollInterval)
      .withSessionTimeout(config.sessionTimeout)
      // Incremental rebalancing: only the partitions that move are revoked, the others keep
      // being processed through the rebalance.
      .withProperty(
        KafkaConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        classOf[CooperativeStickyAssignor].getName
      )
      // On revoke, wait (up to the session timeout) for the partition's stream to finish its
      // in-flight records, and commit offsets already handed to commit before the partition
      // moves. Both cut redeliveries after a rebalance.
      .withRebalanceRevokeMode(RebalanceRevokeMode.Graceful)
      .withCommitOnRevoke(true)
      .withProperties(config.extraProperties)

  extension [A](a: A) private def pipe[B](f: A => B): B = f(a)

}

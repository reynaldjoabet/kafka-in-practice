package kafkainpractice.client

import scala.concurrent.duration.*

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import cats.Parallel
import fs2.{Chunk, Pipe}
import fs2.kafka.*

import org.apache.kafka.clients.producer.{ProducerConfig as KafkaProducerConfig, RecordMetadata}

/**
  * Producer tuning. The defaults favour durability and ordering first, then throughput.
  *
  * @param linger
  *   how long a partition's batch may wait for more records before it is sent. A few milliseconds
  *   of delay buys much larger batches
  * @param batchSizeBytes
  *   upper bound on one partition's batch
  * @param compression
  *   applied per batch, so it pays off together with `linger`
  * @param deliveryTimeout
  *   total time Kafka may spend delivering a record, retries included. This is the bound on
  *   retries: the client retries transient errors on its own until this runs out, so the
  *   application should not add its own retry loop on top
  * @param requestTimeout
  *   how long to wait for one produce request. Must be well below `deliveryTimeout`
  * @param closeTimeout
  *   how long shutdown waits for buffered records to be delivered
  */
final case class ProducerConfig(
    connection: KafkaConnection,
    linger: FiniteDuration = 10.millis,
    batchSizeBytes: Int = 64 * 1024,
    compression: Compression = Compression.Lz4,
    deliveryTimeout: FiniteDuration = 2.minutes,
    requestTimeout: FiniteDuration = 30.seconds,
    closeTimeout: FiniteDuration = 30.seconds,
    extraProperties: Map[String, String] = Map.empty
)

/**
  * A thread-safe producer. Create one per application and share it: each instance owns a buffer, an
  * I/O thread and connections to every broker it writes to.
  *
  * Every `send` method returns only after the brokers have acknowledged the records, so a
  * successful result means the records are durably written (`acks=all`).
  */
final class EventProducer[F[_]: Async, K, V] private (producer: KafkaProducer[F, K, V]) {

  /**
    * Sends one record and waits for it to be acknowledged.
    */
  def send(record: ProducerRecord[K, V]): F[RecordMetadata] =
    sendAll(Chunk.singleton(record)).map(_(0))

  def send(topic: String, key: K, value: V): F[RecordMetadata] =
    send(ProducerRecord(topic, key, value))

  /**
    * Sends records together and waits for all of them. Records for the same partition keep their
    * relative order. Fails on the first record that fails.
    */
  def sendAll(records: Chunk[ProducerRecord[K, V]]): F[Chunk[RecordMetadata]] =
    producer.produce(records).flatten.map(_.map(_._2))

  /**
    * Buffers the records and returns an effect that waits for their acknowledgement.
    *
    * Use this to keep several sends in flight: enqueue many, then wait. Waiting on each record
    * before sending the next limits throughput to one round trip per record.
    */
  def enqueue(records: Chunk[ProducerRecord[K, V]]): F[F[Chunk[RecordMetadata]]] =
    producer.produce(records).map(_.map(_.map(_._2)))

  /**
    * Sends each incoming chunk as a batch, keeping many batches in flight, and emits the results in
    * input order.
    */
  def pipe: Pipe[F, ProducerRecords[K, V], ProducerResult[K, V]] =
    KafkaProducer.pipe(producer)

}

/**
  * Values of the producer's `compression.type`.
  */
enum Compression(val configValue: String) {

  case None   extends Compression("none")
  case Gzip   extends Compression("gzip")
  case Snappy extends Compression("snappy")
  case Lz4    extends Compression("lz4")
  case Zstd   extends Compression("zstd")

}

object EventProducer {

  def resource[F[_]: Async: Parallel, K, V](
      config: ProducerConfig,
      keySerializer: KeySerializer[F, K],
      valueSerializer: ValueSerializer[F, V]
  ): Resource[F, EventProducer[F, K, V]] =
    KafkaProducer
      .resource(settings(config, keySerializer, valueSerializer))
      .map(new EventProducer(_))

  def settings[F[_], K, V](
      config: ProducerConfig,
      keySerializer: KeySerializer[F, K],
      valueSerializer: ValueSerializer[F, V]
  ): ProducerSettings[F, K, V] =
    ProducerSettings(keySerializer, valueSerializer)
      .withBootstrapServers(config.connection.bootstrapServers)
      .withClientId(config.connection.clientId)
      .withProperties(config.connection.properties)
      // Wait for every in-sync replica. With min.insync.replicas=2 on the topic, an
      // acknowledged record survives the loss of any one broker.
      .withAcks(Acks.All)
      // The broker drops retried duplicates by producer id and sequence number, and keeps
      // batches in order even with several requests in flight. Kafka requires acks=all and at
      // most 5 in-flight requests per connection for this.
      .withEnableIdempotence(true)
      .withMaxInFlightRequestsPerConnection(5)
      .withLinger(config.linger)
      .withBatchSize(config.batchSizeBytes)
      .withProperty(KafkaProducerConfig.COMPRESSION_TYPE_CONFIG, config.compression.configValue)
      .withRequestTimeout(config.requestTimeout)
      .withDeliveryTimeout(config.deliveryTimeout)
      .withCloseTimeout(config.closeTimeout)
      // Report a failed record as soon as its callback fails instead of after the whole batch.
      .withFailFastProduce(true)
      .withProperties(config.extraProperties)

}

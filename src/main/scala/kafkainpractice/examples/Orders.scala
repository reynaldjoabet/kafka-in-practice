package kafkainpractice.examples

import cats.data.NonEmptyList
import cats.effect.{IO, IOApp}
import fs2.kafka.*
import fs2.Chunk

import kafkainpractice.client.*
import org.typelevel.log4cats.slf4j.Slf4jLogger

// Run against a local broker:
//   sbt --client "runMain kafkainpractice.examples.ProduceOrders"
//   sbt --client "runMain kafkainpractice.examples.ConsumeOrders"
// BOOTSTRAP_SERVERS overrides localhost:9092.

private val bootstrapServers = sys.env.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092")
private val ordersTopic      = "orders"

object ProduceOrders extends IOApp.Simple {

  private val config =
    ProducerConfig(KafkaConnection(bootstrapServers, clientId = "orders-producer"))

  def run: IO[Unit] =
    EventProducer
      .resource(config, Serializer.string[IO], Serializer.string[IO])
      .use { producer =>
        // Keyed by customer, so all orders of one customer land in one partition, in order.
        val records = Chunk.from(1 to 100).map { i =>
          ProducerRecord(ordersTopic, s"customer-${i % 10}", s"order-$i")
        }
        producer.sendAll(records).flatMap { acks =>
          IO.println(s"${acks.size} orders acknowledged, last at ${acks.last.map(_.offset)}")
        }
      }

}

object ConsumeOrders extends IOApp.Simple {

  private val config = ConsumerConfig(
    KafkaConnection(bootstrapServers, clientId = "orders-billing"),
    groupId = "billing",
    topics = NonEmptyList.one(ordersTopic),
    deadLetterTopic = Some(s"$ordersTopic.billing.dlq")
  )

  def run: IO[Unit] =
    Slf4jLogger.create[IO].flatMap { logger =>
      EventConsumer
        .resource(config, Deserializer.string[IO], Deserializer.string[IO])
        .use(_.run { record =>
          logger
            .info(s"billing ${record.key}: ${record.value} (${record.partition}@${record.offset})")
        })
    }

}

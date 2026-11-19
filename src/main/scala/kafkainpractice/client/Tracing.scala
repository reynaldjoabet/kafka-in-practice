package kafkainpractice.client

import cats.effect.Concurrent
import cats.syntax.all.*
import cats.Parallel
import fs2.kafka.otel4s.trace.{KafkaMessageKey, KafkaTracer}

import org.typelevel.otel4s.trace.{Tracer, TracerProvider}

/**
  * OpenTelemetry tracing for [[EventProducer]] and [[EventConsumer]].
  *
  *   - `kafka` (fs2-kafka-otel4s) creates the messaging spans: `send <topic>` on the producer, with
  *     the trace context written into the record headers, and `process <topic>` around each
  *     consumed record, linked to the producer's span through those headers.
  *   - `tracer` is used by the consumer to annotate the current `process` span: retries and
  *     dead-lettering are recorded as span events, so one span shows everything that happened to a
  *     record.
  *
  * Both clients take a `Tracing` as a `using` parameter. Without one in scope, the companion's
  * no-op instance is used and nothing is traced. To trace, put one in scope:
  *
  * {{{
  * OtelJava.autoConfigured[IO]().evalMap(otel => Tracing.create[IO](using otel.tracerProvider))
  *   .use { tracing =>
  *     given Tracing[IO] = tracing
  *     EventProducer.resource(...).use(...)
  *   }
  * }}}
  */
final case class Tracing[F[_]](kafka: KafkaTracer[F], tracer: Tracer[F])

object Tracing {

  /**
    * The instrumentation scope name of the spans and events this project adds itself.
    */
  val InstrumentationName = "kafkainpractice"

  def create[F[_]: Concurrent: Parallel: TracerProvider](
      config: KafkaTracer.Config = KafkaTracer.Config.default
  ): F[Tracing[F]] =
    (KafkaTracer.create[F](config), TracerProvider[F].get(InstrumentationName)).mapN(Tracing(_, _))

  def noop[F[_]: Concurrent: Parallel]: Tracing[F] =
    Tracing(KafkaTracer.noop[F], Tracer.noop[F])

  /**
    * The default when no `Tracing` is in scope: trace nothing. It lives in the companion, so a
    * `given Tracing[F]` in the caller's scope always takes precedence.
    */
  given noopTracing[F[_]: Concurrent: Parallel]: Tracing[F] = noop[F]

  /**
    * Span attribute `messaging.kafka.message.key` for raw byte keys: omitted, because the bytes may
    * not be text. Used by the consumer and the dead-letter producer, which both work on raw bytes.
    */
  given rawKey: KafkaMessageKey[Array[Byte]] = KafkaMessageKey.instance(_ => None)

  /**
    * For key types without a `KafkaMessageKey` instance (the library has String, Int, Long and
    * UUID): the key attribute is omitted instead of requiring every caller to define one.
    */
  def unknownKey[K]: KafkaMessageKey[K] = KafkaMessageKey.instance(_ => None)

}

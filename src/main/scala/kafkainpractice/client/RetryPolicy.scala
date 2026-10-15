package kafkainpractice.client

import scala.concurrent.duration.*

import cats.effect.std.Random
import cats.effect.Temporal
import cats.syntax.all.*

/**
  * Exponential backoff with full jitter: before retry `n` (starting at 1), wait a random time
  * between zero and `min(maxBackoff, initialBackoff * 2^(n-1))`. The jitter keeps many consumers
  * that failed at the same moment from retrying in lockstep.
  *
  * @param maxRetries
  *   retries after the first attempt; `0` disables retrying
  */
final case class RetryPolicy(
    maxRetries: Int = 5,
    initialBackoff: FiniteDuration = 200.millis,
    maxBackoff: FiniteDuration = 30.seconds
) {

  require(maxRetries >= 0, "maxRetries must be >= 0")
  require(initialBackoff > Duration.Zero, "initialBackoff must be > 0")
  require(maxBackoff >= initialBackoff, "maxBackoff must be >= initialBackoff")

  /**
    * Upper bound of the wait before retry `n`.
    */
  def backoffCap(n: Int): FiniteDuration = {
    // Cap the exponent so the shift cannot overflow; maxBackoff wins long before that.
    val factor = 1L << math.min(n - 1, 30)
    val nanos  = initialBackoff.toNanos
    if nanos > maxBackoff.toNanos / factor then maxBackoff
    else (nanos * factor).nanos
  }

  /**
    * Runs `fa`, retrying failures that `retryable` accepts. Calls `onRetry` with the attempt number
    * and error before each wait. On giving up, raises the last error together with the number of
    * attempts made.
    */
  def retry[F[_]: Temporal: Random, A](
      fa: F[A],
      retryable: Throwable => Boolean,
      onRetry: (Int, Throwable) => F[Unit]
  ): F[Either[(Throwable, Int), A]] = {
    def go(attempt: Int): F[Either[(Throwable, Int), A]] =
      fa.attempt.flatMap {
        case Right(a)                                         => a.asRight.pure
        case Left(e) if attempt <= maxRetries && retryable(e) =>
          val cap = backoffCap(attempt)
          onRetry(attempt, e) >>
            Random[F].betweenLong(0L, cap.toNanos + 1).flatMap(n => Temporal[F].sleep(n.nanos)) >>
            go(attempt + 1)
        case Left(e) => (e, attempt).asLeft.pure
      }
    go(1)
  }

}

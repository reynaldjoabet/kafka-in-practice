package kafkainpractice.client

import scala.concurrent.duration.*

import cats.effect.kernel.Ref
import cats.effect.std.Random
import cats.effect.IO

import munit.CatsEffectSuite

class RetryPolicySpec extends CatsEffectSuite {

  test("backoff doubles from the initial value and stops at the maximum") {
    val policy = RetryPolicy(maxRetries = 100, initialBackoff = 100.millis, maxBackoff = 1.second)
    assertEquals(
      (1 to 6).map(policy.backoffCap).toList,
      List(100.millis, 200.millis, 400.millis, 800.millis, 1.second, 1.second)
    )
  }

  test("backoff does not overflow for very high attempt numbers") {
    val policy = RetryPolicy(maxRetries = Int.MaxValue, initialBackoff = 1.day, maxBackoff = 7.days)
    assertEquals(policy.backoffCap(10_000), 7.days)
  }

  test("retries until the action succeeds") {
    val policy = RetryPolicy(maxRetries = 5, initialBackoff = 1.milli, maxBackoff = 2.millis)
    for {
      given Random[IO] <- Random.scalaUtilRandom[IO]
      calls            <- Ref[IO].of(0)
      action            =
        calls.updateAndGet(_ + 1).flatMap(n => IO.raiseWhen(n < 3)(new Exception(s"fail $n")))
      result <- policy.retry(action, _ => true, (_, _) => IO.unit)
      n      <- calls.get
    } yield {
      assertEquals(result, Right(()))
      assertEquals(n, 3)
    }
  }

  test("gives up after maxRetries and reports the number of attempts") {
    val policy = RetryPolicy(maxRetries = 2, initialBackoff = 1.milli, maxBackoff = 2.millis)
    for {
      given Random[IO] <- Random.scalaUtilRandom[IO]
      result           <-
        policy.retry(IO.raiseError[Unit](new Exception("boom")), _ => true, (_, _) => IO.unit)
    } yield assertEquals(
      result.left.map { case (e, attempts) => (e.getMessage, attempts) },
      Left(("boom", 3))
    )
  }

  test("does not retry errors the predicate rejects") {
    val policy = RetryPolicy(maxRetries = 5, initialBackoff = 1.milli, maxBackoff = 2.millis)
    for {
      given Random[IO] <- Random.scalaUtilRandom[IO]
      result           <- policy.retry(
                  IO.raiseError[Unit](new IllegalArgumentException),
                  _ => false,
                  (_, _) => IO.unit
                )
    } yield assertEquals(result.left.map(_._2), Left(1))
  }

}

package com.evolutiongaming.kafka.flow

import cats.effect.Concurrent
import cats.effect.Resource
import cats.effect.Timer
import cats.effect.implicits._
import cats.syntax.all._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.OnError
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.retry.Strategy
import com.evolutiongaming.sstream.Stream
import kafka.Consumer
import scala.concurrent.duration._

object KafkaFlow {

  /** Process records from consumer with default retry strategy
    *
    * Returns records already processed by the `KafkaFlow`.
    *
    * Note, that returned record does not guarantee that commit to
    * Kafka happened, i.e. that the record will not be processsed for the
    * second time.
    *
    * WARNING: Do not forget to `flatMap` returned `F[Unit]` or the
    * potential errors may be lost.
    */
  def retryOnError[F[_]: Concurrent: Timer: LogOf](
    consumer: Resource[F, Consumer[F]],
    flowOf: ConsumerFlowOf[F],
  ): Resource[F, F[Unit]] = {

    val retry = for {
      random <- Random.State.fromClock[F]()
      log <- LogOf[F].apply(KafkaFlow.getClass)
    } yield Retry(
      strategy =
        Strategy
        .exponential(100.millis)
        .jitter(random)
        .cap(1.minute)
        .resetAfter(5.minutes),
      onError =
        OnError.fromLog(log)
    )

    Resource.eval(retry) flatMap { implicit retry =>
      resource(consumer, flowOf)
    }

  }

  /** Process records from consumer with given flow and retry strategy
    *
    * Returns records already processed by the `KafkaFlow`.
    *
    * Note, that returned record does not guarantee that commit to
    * Kafka happened, i.e. that the record will not be processsed for the
    * second time.
    */
  def stream[F[_]: BracketThrowable: Retry](
    consumer: Resource[F, Consumer[F]],
    flowOf: ConsumerFlowOf[F],
  ): Stream[F, ConsRecords] =
    for {
      _        <- Stream.around(Retry[F].toFunctionK)
      consumer <- Stream.fromResource(consumer)
      flow     <- Stream.fromResource(flowOf(consumer))
      records  <- flow.stream
    } yield records

  /** Process records from consumer with given flow and retry strategy
    *
    * Tears down if cancelled or retry strategy failed.
    *
    * WARNING: Do not forget to `flatMap` returned `F[Unit]` or the
    * potential errors may be lost.
    */
  def resource[F[_]: Concurrent: Retry](
    consumer: Resource[F, Consumer[F]],
    flowOf: ConsumerFlowOf[F],
  ): Resource[F, F[Unit]] =
    stream(consumer, flowOf).drain.background

}

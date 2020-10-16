package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.effect.Resource
import cats.effect.Sync
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.timer.TimerFlowOf
import persistence.Persistence
import timer.TimerContext

trait KeyFlowOf[F[_], S, A] {

  def apply(
    context: KeyContext[F],
    persistence: Persistence[F, S, A],
    timers: TimerContext[F]
  ): Resource[F, KeyFlow[F, A]]

}
object KeyFlowOf {

  def apply[F[_]: Sync, K, S, A](
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S]
  ): KeyFlowOf[F, S, A] = { (context, persistence, timers) =>
    implicit val _context = context
    Resource.liftF {
      timerFlowOf(context, persistence, timers) flatMap { timerFlow =>
        KeyFlow.of(fold, tick, persistence, timerFlow)
      }
    }
  }

  def apply[F[_]: Sync, K, S, A](
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, A]
  ): KeyFlowOf[F, S, A] =
    KeyFlowOf(timerFlowOf, fold, TickOption.id)

}
package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.Monad
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.mtl.MonadState
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.persistence.Persistence
import com.evolutiongaming.kafka.flow.timer.ReadTimestamps
import com.evolutiongaming.kafka.flow.timer.TimerContext
import com.evolutiongaming.kafka.flow.timer.Timers
import com.evolutiongaming.kafka.flow.timer.WriteTimestamps
import com.olegpy.meow.effects._
import timer.TimerFlow

trait KeyFlow[F[_], A] {

  /** Update timestamps of the flow */
  def timestamps: WriteTimestamps[F]

  /** Process incoming records */
  def onRecords: RecordFlow[F, A]

  /** Triggers all the timers which were registered to trigger at or before current timestamp */
  def trigger: F[Unit]

}

object KeyFlow {

  /** Create flow which persists snapshots, events and restores state if needed */
  def of[F[_]: Sync: KeyContext: TimerContext, S, A](
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S],
    persistence: Persistence[F, S, A],
    timerFlow: TimerFlow[F]
  ): F[KeyFlow[F, A]] = Ref.of(none[S]) flatMap { storage =>
    of(storage.stateInstance, fold, tick, persistence, timerFlow)
  }

  /** Create flow which persists snapshots, events and restores state if needed */
  def of[F[_]: Monad: KeyContext: TimerContext, S, A](
    storage: MonadState[F, Option[S]],
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S],
    persistence: Persistence[F, S, A],
    timerFlow: TimerFlow[F]
  ): F[KeyFlow[F, A]] = for {
    state <- persistence.read(KeyContext[F].log)
    _ <- storage.set(state)
    foldToState = FoldToState(storage, fold, persistence)
    tickToState = TickToState(storage, tick, persistence)
    _timerFlow = TimerFlow(tickToState.run *> timerFlow.onTimer)
  } yield new KeyFlow[F, A] {
    def timestamps = WriteTimestamps[F]
    def onRecords = foldToState(_)
    def trigger = Timers[F].trigger(_timerFlow)
  }


  /** Does not save anything to the database.
    *
    * It also does not allow Kafka to commit until they key is fully processed.
    * This flow could be used for the journals which are guaranteed to have
    * the last event in a relatively short time.
    *
    * If there is a chance such final event will not come, other flow
    * constuctors might be a better choice to avoid blocking commits to Kafka
    * forever.
    */
  def transient[F[_]: Sync: KeyContext: TimerContext, K, S, A](
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S],
    timerFlow: TimerFlow[F]
  ): F[KeyFlow[F, A]] =
    for {
      startedAt <- ReadTimestamps[F].current
      _ <- KeyContext[F].hold(startedAt.offset)
      storage <- Ref.of(none[S])
      foldToState = FoldToState(storage.stateInstance, fold, Persistence.empty[F, S, A])
      tickToState = TickToState(storage.stateInstance, tick, Persistence.empty[F, S, A])
      _timerFlow = TimerFlow(tickToState.run *> timerFlow.onTimer)
    } yield new KeyFlow[F, A] {
      def timestamps = WriteTimestamps[F]
      def onRecords = foldToState(_)
      def trigger = Timers[F].trigger(_timerFlow)
    }

  /** Ignores records, timestamp writes and triggers */
  def empty[F[_]: Applicative, A]: KeyFlow[F, A] = new KeyFlow[F, A] {
    def timestamps = WriteTimestamps.empty[F]
    def onRecords = RecordFlow.empty[F, A]
    def trigger = ().pure[F]
  }

}
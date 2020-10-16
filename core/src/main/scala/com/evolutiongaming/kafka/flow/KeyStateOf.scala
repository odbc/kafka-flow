package com.evolutiongaming.kafka.flow

import cats.effect.Resource
import cats.effect.Sync
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.timer.TimerContext
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.sstream.Stream
import key.KeysOf
import persistence.Persistence
import persistence.PersistenceOf
import persistence.SnapshotPersistenceOf
import timer.TimerFlowOf
import timer.TimersOf
import timer.Timestamp

private[flow] trait KeyStateOf[F[_], K, A] { self =>

  /** Creates or restores a state for a single key */
  def apply(
    key: K,
    createdAt: Timestamp,
    context: KeyContext[F]
  ): Resource[F, KeyState[F, A]]

  /** Transforms `K` parameter into something else.
    *
    * See also `Invariant#imap`.
    */
  def contramap[L](g: L => K): KeyStateOf[F, L, A] = new KeyStateOf[F, L, A] {
    def apply(key: L, createdAt: Timestamp, context: KeyContext[F]) =
      self.apply(g(key), createdAt, context)
  }

}
object KeyStateOf {

  /** Creates or recovers the key state.
    *
    * This version allows one to construct a custom `KeyFlowOf`
    * for generic persistence.
    */
  def apply[F[_]: Sync, K, S, A](
    timersOf: TimersOf[F, K],
    persistenceOf: PersistenceOf[F, K, S, A],
    keyFlowOf: KeyFlowOf[F, S, A],
    recover: FoldOption[F, S, A]
  ): KeyStateOf[F, K, A] = { (key, createdAt, context) =>
    for {
      timers <- Resource.liftF(timersOf(key, createdAt))
      persistence <- Resource.liftF(persistenceOf(key, recover, timers))
      keyFlow <- keyFlowOf(context, persistence, timers)
    } yield KeyState(keyFlow, timers, context.holding)
  }

}
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

trait KeyStateOf[F[_], K, A] { self =>

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

  /** Transforms returned `KeyState` to something else.
    *
    * Could be used to count allocations for example.
    */
  def mapResource[B](
    f: Resource[F, KeyState[F, A]] => Resource[F, KeyState[F, B]]
  ): KeyStateOf[F, K, B] = new KeyStateOf[F, K, B] {
    def apply(key: K, createdAt: Timestamp, context: KeyContext[F]) =
      f(self.apply(key, createdAt, context))
  }

}
object KeyStateOf {

  /** Creates or recovers the key state.
    *
    * This version only requires `TimerFlowOf` and uses default `RecordFlow`
    * which reads the state from the generic persistence folds it using
    * default `FoldToState`.
    */
  def apply[F[_]: Sync, K, S, A](
    timersOf: TimersOf[F, K],
    persistenceOf: PersistenceOf[F, K, S, A],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S]
  ): KeyStateOf[F, K, A] = apply(
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    keyFlowOf = { (context, persistence: Persistence[F, S, A], timers) =>
      implicit val _context = context
      timerFlowOf(context, persistence, timers) flatMap { timerFlow =>
        KeyFlow.of(fold, tick, persistence, timerFlow)
      }
    },
    recover = fold
  )

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
    val keyState = for {
      timers <- timersOf(key, createdAt)
      persistence <- persistenceOf(key, recover, timers)
      keyFlow <- keyFlowOf(context, persistence, timers)
    } yield KeyState(keyFlow, timers, context.holding)
    Resource.liftF(keyState)
  }

}
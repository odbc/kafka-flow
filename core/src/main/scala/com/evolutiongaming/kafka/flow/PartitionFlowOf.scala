package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.effect.Concurrent
import cats.effect.Resource
import cats.effect.Resource
import cats.effect.Timer
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.sstream.Stream
import key.KeysOf
import persistence.Persistence
import persistence.PersistenceOf
import persistence.SnapshotPersistenceOf
import timer.TimerFlowOf
import timer.TimersOf


trait PartitionFlowOf[F[_]] {

  /** Creates partition record handler for assigned partition */
  def apply(topicPartition: TopicPartition, assignedAt: Offset): Resource[F, PartitionFlow[F]]

}
object PartitionFlowOf {

  /** Creates `PartitionFlowOf` for specific application */
  private def eagerRecovery[F[_]: Concurrent: Timer: Parallel: LogOf, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    keyStateOf: KeyStateOf[F, KafkaKey, ConsRecord],
    config: PartitionFlowConfig
  ): PartitionFlowOf[F] = { (topicPartition, assignedAt) =>
    PartitionFlow.resource(
      topicPartition = topicPartition,
      assignedAt = assignedAt,
      recoverKeys = keysOf.all(applicationId, groupId, topicPartition) map (_.key),
      keyStateOf = keyStateOf contramap { key =>
        KafkaKey(applicationId, groupId, topicPartition, key)
      },
      config
    )
  }

  /** Creates `PartitionFlowOf` for specific application */
  private def lazyRecovery[F[_]: Concurrent: Timer: Parallel: LogOf, S](
    applicationId: String,
    groupId: String,
    keyStateOf: KeyStateOf[F, KafkaKey, ConsRecord],
    config: PartitionFlowConfig
  ): PartitionFlowOf[F] = { (topicPartition, assignedAt) =>
    PartitionFlow.resource(
      topicPartition = topicPartition,
      assignedAt = assignedAt,
      recoverKeys = Stream.empty,
      keyStateOf = keyStateOf contramap { key =>
        KafkaKey(applicationId, groupId, topicPartition, key)
      },
      config
    )
  }

  /** Does not recover keys until record with such key is encountered.
    *
    * This version only requires `TimerFlowOf` and uses default `RecordFlow`
    * which reads the state from the generic persistence folds it using
    * default `FoldToState`.
    */
  def lazyRecovery[F[_]: Concurrent: Parallel: Timer: LogOf, S](
    applicationId: String,
    groupId: String,
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord],
    config: PartitionFlowConfig
  ): PartitionFlowOf[F] = lazyRecovery(
    applicationId, groupId,
    timersOf, persistenceOf, timerFlowOf,
    fold, TickOption.id,
    config
  )

  /** Does not recover keys until record with such key is encountered.
    *
    * This version only requires `TimerFlowOf` and uses default `RecordFlow`
    * which reads the state from the generic persistence folds it using
    * default `FoldToState`.
    */
  def lazyRecovery[F[_]: Concurrent: Parallel: Timer: LogOf, S](
    applicationId: String,
    groupId: String,
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord],
    tick: TickOption[F, S],
    config: PartitionFlowConfig
  ): PartitionFlowOf[F] = lazyRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keyStateOf = KeyStateOf(
      timersOf = timersOf,
      persistenceOf = persistenceOf,
      keyFlowOf = KeyFlowOf(timerFlowOf, fold, tick),
      recover = fold
    ),
    config = config
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version only requires `TimerFlowOf` and uses default `Keyflow`
    * which reads the state from the generic persistence and folds it using
    * default `FoldToState`.
    *
    * It also uses default implementaion of `Tick` which does nothing and
    * does not touch the state.
    */
  def eagerRecovery[F[_]: Concurrent: Parallel: Timer: LogOf, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord],
    config: PartitionFlowConfig
  ): PartitionFlowOf[F] = eagerRecovery(
    applicationId, groupId, keysOf, timersOf, persistenceOf, timerFlowOf,
    fold, TickOption.id, config
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version only requires `TimerFlowOf` and uses default `Keyflow`
    * which reads the state from the generic persistence and folds it using
    * default `FoldToState`.
    */
  def eagerRecovery[F[_]: Concurrent: Parallel: Timer: LogOf, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord],
    tick: TickOption[F, S],
    config: PartitionFlowConfig
  ): PartitionFlowOf[F] = eagerRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keysOf = keysOf,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    keyFlowOf = { (context, persistence: Persistence[F, S, ConsRecord], timers) =>
      implicit val _context = context
      Resource.liftF(timerFlowOf(context, persistence, timers)) evalMap { timerFlow =>
        KeyFlow.of(fold, tick, persistence, timerFlow)
      }
    },
    recover = fold,
    config = config
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version allows one to construct a custom `KeyFlowOf`
    * for snapshot persistence.
    */
  def eagerRecovery[F[_]: Concurrent: Parallel: Timer: LogOf, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsRecord],
    keyFlowOf: KeyFlowOf[F, S, ConsRecord],
    config: PartitionFlowConfig
  ): PartitionFlowOf[F] = eagerRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keysOf = keysOf,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    keyFlowOf = keyFlowOf,
    recover = FoldOption.empty[F, S, ConsRecord],
    config = config
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version allows one to construct a custom `KeyFlowOf`
    * for generic persistence.
    */
  def eagerRecovery[F[_]: Concurrent: Parallel: Timer: LogOf, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    keyFlowOf: KeyFlowOf[F, S, ConsRecord],
    recover: FoldOption[F, S, ConsRecord],
    config: PartitionFlowConfig
  ): PartitionFlowOf[F] = eagerRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keysOf = keysOf,
    keyStateOf = KeyStateOf(
      timersOf, persistenceOf, keyFlowOf, recover
    ),
    config = config
  )

}
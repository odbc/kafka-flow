---
id: changelog
title: Changelog
sidebar_label: Changelog
---

# 0.4.x

## Breaking changes

- `KeyStateOf` is now private, use appropriate constructor on `PartitionFlowOf` now,
- `KeyStateOf.mapResource` is removed now, use `key_flow_count` gauge from `KeyFlowMetrics` instead,
i.e. do the following:
```scala mdoc:silent
import cats.effect.IO
import com.evolutiongaming.kafka.flow.KeyFlowOf
import com.evolutiongaming.smetrics.MeasureDuration

type F[T] = IO[T]
type S = String
type A = String
implicit val measureDuration = MeasureDuration.empty[IO]
def keyFlowOf: KeyFlowOf[F, S, A] = ???
```
```scala mdoc
import com.evolutiongaming.kafka.flow.KeyFlowMetrics._
import com.evolutiongaming.kafka.flow.metrics.syntax._

def keyFlowWithMetrics = keyFlowOf.withCollectorRegistry[F](???)
```

# 0.2.x

## New features

- `PersistenceModule` trait to pass around persistence for all of keys, journals
and snapshots together and minimize the boilerplate. `CassandraPersistence` class
is a first implementation.
- `PartitionFlow`, `CassandraKeys`, `CassandraJournals` and `CassandraSnapshots`
do not require `MeasureDuration` anymore as it was not used anyway.

## Breaking changes

- `CassandraKeys.withSchema` requires `MonadThrowable` instead of `Fail` to
minimize custom DSL.
- `KafkaModule` renamed to `ConsumerModule` to reflect the purpose.
- `Tick.unit` and `TickOption.unit` renamed to `Tick.id` and `TickUnit.id`.
- `PartitionFlowOf.eagerRecoveryKafkaPersistence` in `kafka-flow-persistence-kafka`
module accepts `Tick` and `Fold` instead of `KeyFlowOf`.
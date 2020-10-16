package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.data.NonEmptyList
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.metrics.MetricsOf
import com.evolutiongaming.kafka.flow.metrics.syntax._
import com.evolutiongaming.kafka.flow.persistence.Persistence
import com.evolutiongaming.kafka.flow.timer.TimerContext
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.smetrics.LabelNames
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.Quantile
import com.evolutiongaming.smetrics.Quantiles
import cats.effect.Resource

object KeyFlowMetrics {

  implicit def keyFlowMetricsOf[F[_]: Monad: MeasureDuration, A]: MetricsOf[F, KeyFlow[F, A]] = { registry =>
    for {
      applySummary <- registry.summary(
        name = "key_flow_apply_duration_seconds",
        help = "Time required to apply a batch coming to key flow",
        quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
        labels = LabelNames()
      )
      onTimerSummary <- registry.summary(
        name = "key_flow_ontimer_duration_seconds",
        help = "Time required to react on triggered timers by key flow",
        quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
        labels = LabelNames()
      )
    } yield { keyFlow =>
      new KeyFlow[F, A] {

        def apply(records: NonEmptyList[A]) =
          keyFlow(records) measureDuration { duration =>
            applySummary.observe(duration.toNanos.nanosToSeconds)
          }

        def onTimer =
          onTimer measureDuration { duration =>
            onTimerSummary.observe(duration.toNanos.nanosToSeconds)
          }

      }
    }
  }

  implicit def keyFlowOfMetricsOf[F[_]: Monad: MeasureDuration, S, A]: MetricsOf[F, KeyFlowOf[F, S, A]] = { registry =>
    registry.gauge(
      name = "key_flow_count",
      help = "The number of active key flows",
      labels = LabelNames()
    ) map { countGauge => keyFlowOf => (context, persistence, timers) =>
      val count = Resource.make(countGauge.inc()) { _ => countGauge.dec() }
      val keyFlow = keyFlowOf(context, persistence, timers)
      count *> keyFlow flatMap (_.withCollectorRegistry(registry))
    }
  }

}
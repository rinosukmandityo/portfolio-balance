//nolint:gochecknoglobals
package monitoring

import (
	kitprom "github.com/go-kit/kit/metrics/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "company"
	subsystem = "portfolio_balance"
)

var defaultBucket = []float64{0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.024, 2.048, 4.096, 8.192, 16.384, 32.768, 60}

// ConsumeDurationSeconds holds duration of consuming kafka process.
var ConsumeDurationSeconds = kitprom.NewHistogramFrom(prometheus.HistogramOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "consume_duration_seconds",
	Help:      "Consume duration in seconds.",
	Buckets:   defaultBucket,
}, []string{"error", "consumer"})

// ConsumeTotal holds total of consumed kafka process.
var ConsumeTotal = kitprom.NewCounterFrom(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "consume_total",
	Help:      "Consume total messages.",
}, []string{"error", "consumer"})

// ConsumeLagSeconds holds Kafka reader lag data on consuming kafka.
var ConsumeLagSeconds = kitprom.NewHistogramFrom(prometheus.HistogramOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "consume_lag_seconds",
	Help:      "Consume lag seconds.",
	Buckets:   defaultBucket,
}, []string{"consumer"})

// StorageDurationSeconds holds storage operation latency.
var StorageDurationSeconds = kitprom.NewHistogramFrom(prometheus.HistogramOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "storage_duration_seconds",
	Help:      "Storage duration seconds.",
	Buckets:   defaultBucket,
}, []string{"error", "method"})

// StorageBulkInsertBlock holds total of consumed kafka process.
var StorageBulkInsertBlock = kitprom.NewCounterFrom(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "storage_bulk_insert_block",
	Help:      "Storage bulk insert block counter.",
}, []string{"path"})

// KafkaFetchMessageDurationSeconds Kafka fetch message duration seconds.
var KafkaFetchMessageDurationSeconds = kitprom.NewHistogramFrom(prometheus.HistogramOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "kafka_fetch_message_duration_seconds",
	Help:      "Kafka fetch message duration seconds.",
	Buckets:   defaultBucket,
}, []string{"error", "topic"})

// KafkaAckMessageDurationSeconds Kafka ack duration seconds.
var KafkaAckMessageDurationSeconds = kitprom.NewHistogramFrom(prometheus.HistogramOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "kafka_ack_duration_seconds",
	Help:      "Kafka ack duration seconds.",
	Buckets:   defaultBucket,
}, []string{"error", "topic"})

// KafkaWriteMessageDurationSeconds Kafka write message duration seconds.
var KafkaWriteMessageDurationSeconds = kitprom.NewHistogramFrom(
	prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "kafka_write_message_duration_seconds",
		Help:      "Kafka write message duration seconds.",
		Buckets:   defaultBucket,
	},
	[]string{"error", "topic"},
)

// KafkaWriteMessageTotal Kafka write message total.
// nolint:gochecknoglobals
var KafkaWriteMessagesTotal = kitprom.NewCounterFrom(
	prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "kafka_write_messages_total",
		Help:      "Kafka write messages total.",
	},
	[]string{"error", "topic"},
)

package kafka

import (
	"context"
	"strconv"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/segmentio/kafka-go"

	"github.com/rinosukmandityo/portfolio-balance/internal/kafka/client"
)

type kafkaMsg struct {
	reader          client.ClientReader
	msg             kafka.Message
	metricCommitMsg metrics.Histogram
}

func (m *kafkaMsg) Msg() []byte {
	return m.msg.Value
}

func (m *kafkaMsg) Timestamp() time.Time {
	return m.msg.Time
}

func (m *kafkaMsg) Partition() int {
	return m.msg.Partition
}

func (m *kafkaMsg) Offset() int64 {
	return m.msg.Offset
}

func (m *kafkaMsg) Commit(ctx context.Context) (err error) {
	start := time.Now()

	defer func() {
		label := []string{
			"topic", m.msg.Topic,
			"error", strconv.FormatBool(err != nil),
		}
		m.metricCommitMsg.With(label...).Observe(time.Since(start).Seconds())
	}()

	return m.reader.CommitMessages(ctx, m.msg)
}

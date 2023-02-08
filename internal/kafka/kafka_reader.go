package kafka

import (
	"context"
	"time"

	"github.com/go-kit/kit/metrics"
	"go.uber.org/zap"

	"github.com/rinosukmandityo/portfolio-balance"
	"github.com/rinosukmandityo/portfolio-balance/internal/config"
	"github.com/rinosukmandityo/portfolio-balance/internal/kafka/client"
)

// KafkaReader is a reader for data point consumer.
type KafkaReader struct {
	rdr             client.ClientReader
	metricCommitMsg metrics.Histogram
	logger          *zap.Logger
}

// NewKafkaReader new command reader.
func NewKafkaReader(
	rdr client.ClientReader,
	logger *zap.Logger,
	metricCommitMsg metrics.Histogram,
) (*KafkaReader, error) {
	dpReader := &KafkaReader{
		rdr:             rdr,
		metricCommitMsg: metricCommitMsg,
		logger:          logger,
	}

	return dpReader, nil
}

// FetchMessage fetchs kafka message.
func (r *KafkaReader) FetchMessage(ctx context.Context) (portfolio.KafkaMsg, error) {
	msg, err := r.rdr.FetchMessage(ctx)
	if err != nil {
		r.logger.Error("unable to fetch kafka messages", zap.Error(err))

		return nil, err
	}

	ret := &kafkaMsg{
		reader:          r.rdr,
		msg:             msg,
		metricCommitMsg: r.metricCommitMsg,
	}

	return ret, nil
}

// Close closes kafka reader.
func (r *KafkaReader) Close() error {
	return r.rdr.Close()
}

// NewKafkaReaderFunc creates a new kafka reader function to register a new kafka consumer.
func NewKafkaReaderFunc(
	logger *zap.Logger,
	cfg config.Kafka,
	metricFetchMsg metrics.Histogram,
	metricCommitMsg metrics.Histogram,
) func() (portfolio.KafkaReader, error) {
	kafkaDialer, err := client.NewDialer(
		cfg.User,
		cfg.Password,
		time.Second*10,
	)
	if err != nil {
		zap.L().Fatal("kafka dialer error", zap.Error(err))
	}

	kafkaRdr, err := client.NewReader(kafkaDialer, &client.ReaderConfig{
		Logger:         logger,
		Topic:          cfg.Topic,
		Brokers:        cfg.Brokers,
		StartOffset:    cfg.StartOffset,
		GroupID:        cfg.Group,
		MinBytes:       cfg.MinBytes,
		MaxBytes:       cfg.MaxBytes,
		MaxWait:        cfg.MaxWait,
		MetricFetchMsg: metricFetchMsg,
	})
	if err != nil {
		zap.L().Fatal("could not create kafka reader", zap.Error(err))
	}

	kafkaReader, err := NewKafkaReader(kafkaRdr, logger, metricCommitMsg)
	if err != nil {
		zap.L().Fatal("could not create kafka reader", zap.Error(err))
	}

	return func() (portfolio.KafkaReader, error) {
		return kafkaReader, nil
	}
}

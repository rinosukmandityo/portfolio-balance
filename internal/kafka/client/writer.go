package client

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/generic"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"

	"github.com/rinosukmandityo/portfolio-balance/internal/config"
)

// ClientWriter kafka.ClientWriter interface
//
//go:generate moq -out ../../mock/kafka_client_writer.go -pkg mock -fmt goimports . ClientWriter
type ClientWriter interface {
	// WriteMessages writes a batch of messages to the kafka topic configured on this ClientWriter.
	WriteMessages(ctx context.Context, msgs ...kafka.Message) (err error)
	// Close closes WriterImpl
	Close() error
}

// WriterImpl is producer messages to Kafka.
type WriterImpl struct {
	logger            *zap.Logger
	wrt               *kafka.Writer
	cfg               config.Kafka
	metricMsgDuration metrics.Histogram
	metricsMsgTotal   metrics.Counter
}

// Option is an option for Service.
type Option func(*WriterImpl)

// WithMsgDurationMetric configures message written duration metric.
func WithMsgDurationMetric(metric metrics.Histogram) Option {
	return func(svc *WriterImpl) {
		svc.metricMsgDuration = metric
	}
}

// WithMsgTotalMetric configures total messages metric.
func WithMsgTotalMetric(metric metrics.Counter) Option {
	return func(svc *WriterImpl) {
		svc.metricsMsgTotal = metric
	}
}

// NewWriter create new kafka client writer.
func NewWriter(
	logger *zap.Logger,
	cfg config.Kafka,
	ops ...Option,
) (*WriterImpl, error) {
	writer := &WriterImpl{
		logger:            logger,
		cfg:               cfg,
		metricMsgDuration: generic.NewHistogram("nop_msg_duration", 1),
		metricsMsgTotal:   generic.NewCounter("nop_msg_counter"),
	}

	for _, opt := range ops {
		opt(writer)
	}

	// if no broker is set, fail fast
	if len(writer.cfg.Brokers) == 0 {
		return nil, ErrNoBroker
	}

	// if logger is not set, we default to no op log
	if writer.logger == nil {
		writer.logger = zap.NewNop()
	}

	// setup new writer
	writer.wrt = &kafka.Writer{
		Addr:        kafka.TCP(writer.cfg.Brokers...),
		Topic:       writer.cfg.Topic,
		Balancer:    &kafka.Hash{},
		ErrorLogger: newErrorLogger(writer.logger),
	}

	var err error
	var sharedTransport = &kafka.Transport{}

	if writer.cfg.User != "" && writer.cfg.Password != "" {
		sharedTransport.SASL, err = scram.Mechanism(scram.SHA512, writer.cfg.User, writer.cfg.Password)
		if err != nil {
			return nil, err
		}
	}

	writer.wrt.Transport = sharedTransport

	writer.logger.Info("initialized kafka writer",
		zap.String("topic", writer.cfg.Topic),
		zap.String("broker", writer.cfg.Brokers[0]),
	)

	return writer, nil
}

// WriteMessages writes a batch of messages to the kafka topic configured on this writer.
func (r *WriterImpl) WriteMessages(ctx context.Context, msgs ...kafka.Message) (err error) {
	stime := time.Now()
	defer func() {
		labels := []string{
			"error", fmt.Sprintf("%t", err != nil),
			"topic", r.cfg.Topic,
		}
		r.metricMsgDuration.With(labels...).Observe(time.Since(stime).Seconds())
		r.metricsMsgTotal.With(labels...).Add(float64(len(msgs)))
	}()

	return r.wrt.WriteMessages(ctx, msgs...)
}

// Close closes WriterImpl.
func (r *WriterImpl) Close() error {
	if err := r.wrt.Close(); err != nil {
		return fmt.Errorf("cannot close WriterImpl: %w", err)
	}

	return nil
}

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"
)

// ClientReader kafka.Reader interface.
//
//go:generate moq -out ../../mock/kafka_client.go -pkg mock -fmt goimports . ClientReader
type ClientReader interface {
	// FetchMessage reads messages from Kafka storage.
	FetchMessage(ctx context.Context) (msg kafka.Message, err error)
	// CommitMessages commits the list of messages passed as argument. The program
	// may pass a context to asynchronously cancel the commit operation when it was
	// configured to be blocking.
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	// Close closes reader.
	Close() error
}

// ReaderImpl is consumer messages from Kafka.
type ReaderImpl struct {
	cfg            *ReaderConfig
	rdr            *kafka.Reader
	metricFetchMsg metrics.Histogram
}

// NewDialer new Kafka dealer.
func NewDialer(username, password string, timeout time.Duration) (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{
		Timeout:   timeout,
		DualStack: true,
	}

	var err error
	dialer.SASLMechanism, err = scram.Mechanism(scram.SHA512, username, password)
	if err != nil {
		return nil, err
	}

	return dialer, err
}

// NewReader is constructor.
func NewReader(dialer *kafka.Dialer, cfg *ReaderConfig) (*ReaderImpl, error) {
	cfg.validateConfig()
	reader := &ReaderImpl{
		cfg:            cfg,
		metricFetchMsg: cfg.MetricFetchMsg,
	}
	for _, broker := range cfg.Brokers {
		conn, err := dialer.DialContext(context.Background(), "tcp", broker)
		if err != nil {
			return nil, fmt.Errorf("error on connect to %s, %w", broker, err)
		}

		if err = conn.Close(); err != nil {
			return nil, fmt.Errorf("error close connection: %w", err)
		}
	}

	logger := log.New(io.Discard, "", 0)
	if cfg.Logger != nil {
		logger = zap.NewStdLog(cfg.Logger)
	}

	reader.rdr = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Dialer:      dialer,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		StartOffset: cfg.StartOffset,
		Partition:   cfg.Partition,
		MinBytes:    cfg.MinBytes,
		MaxBytes:    cfg.MaxBytes,
		MaxWait:     cfg.MaxWait,
		Logger:      logger,
	})
	// if SpecificOffset is more than 0 than overwrite the reader offset.
	if cfg.SpecificOffset > 0 {
		reader.rdr.SetOffset(cfg.SpecificOffset)
	}
	if cfg.Logger != nil {
		cfg.Logger.Info("reader created",
			zap.String("topic", cfg.Topic),
			zap.String("group_id", cfg.GroupID),
			zap.Int64("start_offset", cfg.StartOffset),
			zap.Int("partition", cfg.Partition),
			zap.Int64("specific_offset", cfg.SpecificOffset),
		)
	}

	return reader, nil
}

// FetchMessage reads messages from Kafka storage.
func (r *ReaderImpl) FetchMessage(ctx context.Context) (msg kafka.Message, err error) {
	start := time.Now()
	defer func() {
		// we do not track DeadlineExceeded error.
		if errors.Is(err, context.DeadlineExceeded) {
			return
		}

		labels := []string{
			"error", strconv.FormatBool(err != nil),
			"topic", r.cfg.Topic,
		}
		r.metricFetchMsg.With(labels...).Observe(time.Since(start).Seconds())
	}()

	return r.rdr.FetchMessage(ctx)
}

// CommitMessages commits the list of messages passed as argument. The program
// may pass a context to asynchronously cancel the commit operation when it was
// configured to be blocking.
func (r *ReaderImpl) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return r.rdr.CommitMessages(ctx, msgs...)
}

// Close closes ReaderImpl.
func (r *ReaderImpl) Close() error {
	if err := r.rdr.Close(); err != nil {
		return fmt.Errorf("cannot close Reader: %w", err)
	}

	return nil
}

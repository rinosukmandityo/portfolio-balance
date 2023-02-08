package pendingdp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/generic"
	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"

	portfolio "github.com/rinosukmandityo/portfolio-balance"
	"github.com/rinosukmandityo/portfolio-balance/internal/consumer/datapoint"
	"github.com/rinosukmandityo/portfolio-balance/internal/kafka/schemas"
)

const (
	defaultConsumerCount  = 8
	defaultBucketSize     = 10000
	defaultBucketWaitTime = time.Second * 3
)

// Service is a kafka consumer service.
type Service struct {
	logger                *zap.Logger
	cancel                context.CancelFunc
	consumersCount        int
	createKafkaReaderFunc portfolio.KafkaReaderFn
	portfolioRepo         portfolio.Repository
	metricMsgDuration     metrics.Histogram
	metricMsgTotal        metrics.Counter
	bucketSize            int
	bucketWaitTime        time.Duration
	messageRPS            rate.Limit
	codec                 *goavro.Codec
	timestampAdjustment   time.Duration
	mu                    sync.Mutex
}

type dataPointMsg struct {
	datapoint *portfolio.DataPointPerGranularity
	kafkaMsg  portfolio.KafkaMsg
}

// NewService creates new kafka consumer group instance.
func NewService(
	logger *zap.Logger,
	createKafkaReaderFunc portfolio.KafkaReaderFn,
	portfolioRepo portfolio.Repository,
	options ...Option,
) (*Service, error) {
	svc := &Service{
		logger:                logger,
		consumersCount:        defaultConsumerCount,
		createKafkaReaderFunc: createKafkaReaderFunc,
		portfolioRepo:         portfolioRepo,
		metricMsgDuration:     generic.NewHistogram("nop_msg_duration", 1),
		metricMsgTotal:        generic.NewCounter("nop_msg_total"),
		bucketSize:            defaultBucketSize,
		bucketWaitTime:        defaultBucketWaitTime,
		messageRPS:            rate.Inf,
	}

	// initialize decoder
	var err error
	svc.codec, err = goavro.NewCodec(schemas.DataPointSchema)
	if err != nil {
		panic(err)
	}

	for _, opt := range options {
		opt(svc)
	}

	return svc, nil
}

// Name svc name.
func (svc *Service) Name() string {
	return "pending-data-point-consumer"
}

// Start starts the kafka consumer.
func (svc *Service) Start(ctx context.Context) error {
	svc.mu.Lock()
	ctx, svc.cancel = context.WithCancel(ctx)
	svc.mu.Unlock()
	svc.logger.Info("consumer initialization", zap.Int("kafka consumers", svc.consumersCount))
	svc.logger.Info("running with time adjustment", zap.Float64("adj_second", svc.timestampAdjustment.Seconds()))
	err := svc.runKafkaConsumers(ctx, svc.consumersCount)
	if err != nil {
		return err
	}
	<-ctx.Done()

	return ctx.Err()
}

// Stop stops svc.
func (svc *Service) Stop(ctx context.Context) {
	svc.mu.Lock()
	svc.cancel()
	svc.mu.Unlock()
}

func (svc *Service) runKafkaConsumers(ctx context.Context, count int) error {
	for i := 0; i < count; i++ {
		reader, err := svc.createKafkaReaderFunc()
		if err != nil {
			svc.mu.Lock()
			svc.cancel()
			svc.mu.Unlock()

			return err
		}
		go svc.runKafkaHandler(ctx, reader)
	}

	return nil
}

func (svc *Service) parseDatapoint(msg []byte) (*portfolio.DataPointMessage, error) {
	var (
		native interface{}
		err    error
	)
	logger := svc.logger.With(
		zap.Float64("adj_second", svc.timestampAdjustment.Seconds()),
	)

	native, _, err = svc.codec.NativeFromBinary(msg)
	if err != nil {
		return nil, fmt.Errorf("cannot decode data point message: %w", err)
	}

	decoded, ok := native.(map[string]interface{})
	if !ok {
		return nil, errors.New("cannot cast kafka message into map[string]interface{}")
	}

	dataPoint, err := schemas.DecodeDataPoint(decoded)
	if err != nil {
		logger := logger.With(
			zap.Any("user_id", decoded["user_id"]),
			zap.Any("currency", decoded["currency"]),
			zap.Any("timestamp", decoded["period"]),
			zap.Any("msg_sent_at", decoded["sent_at"]),
		)
		logger.Error("unable to decode data point from maps to data point message", zap.Error(err))

		return nil, err
	}

	dataPoint.Timestamp = dataPoint.Timestamp.Add(svc.timestampAdjustment).Truncate(time.Minute)

	return dataPoint, err
}

// runKafkaHandler reads kafka message to store a new data point.
//
//nolint:funlen
func (svc *Service) runKafkaHandler(ctx context.Context, kafkaReader portfolio.KafkaReader) {
	hub := sentry.CurrentHub().Clone()
	ctx = sentry.SetHubOnContext(ctx, hub)

	defer kafkaReader.Close()

	msgChan := make(chan *dataPointMsg, svc.bucketSize/10)
	go svc.populateDataPoints(ctx, msgChan)

	limiter := rate.NewLimiter(svc.messageRPS, 1)
	tokenAvailable := 0

	for {
		// token is exhausted, fill it up to bucket size
		if tokenAvailable <= 0 {
			svc.logger.Info("wait until rate is available")
			tWait := time.Now().UTC()
			// wait until rate is available
			err := limiter.Wait(ctx)
			// most likely context canceled
			if err != nil {
				svc.logger.Error("rate limiter stopped", zap.Error(err))
				svc.mu.Lock()
				svc.cancel()
				svc.mu.Unlock()

				return
			}
			svc.logger.Info("rate limit wait done", zap.Duration("duration", time.Since(tWait)))

			tokenAvailable += svc.bucketSize
		}
		kafkaMsg, err := kafkaReader.FetchMessage(ctx)
		if err != nil {
			svc.logger.Error("cannot fetch message from Kafka topic", zap.Error(err))
			svc.mu.Lock()
			svc.cancel()
			svc.mu.Unlock()

			return
		}

		var msg *dataPointMsg
		msg, err = svc.processMessages(ctx, kafkaMsg)
		if err != nil {
			return
		} else if msg != nil {
			// use up 1 token for each fetch
			tokenAvailable--

			msgChan <- msg
		}
	}
}

// populateDataPoints populates data point bucket until particular quota.
// it also has a ticker to wait until particular time regardless the bucket full or not.
// if the quota is full or the wait time is over it will store data point bucket to database.
func (svc *Service) populateDataPoints(
	ctx context.Context,
	msgChan <-chan *dataPointMsg,
) {
	msgs := make([]*dataPointMsg, 0, svc.bucketSize)
	ticker := time.NewTicker(svc.bucketWaitTime)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()

			return
		case msg := <-msgChan:
			msgs = append(msgs, msg)
			if len(msgs) == svc.bucketSize {
				ticker.Stop()
				if err := svc.processTaskWithMetrics(ctx, msgs); err != nil {
					svc.logger.Error("unable to process kafka message", zap.Error(err))
					if portfolio.ErrorCode(err) == portfolio.ErrorCodeInternal {
						svc.mu.Lock()
						svc.cancel()
						svc.mu.Unlock()

						return
					}
				}
				ticker.Reset(svc.bucketWaitTime)
				// reset tasks bucket.
				msgs = msgs[:0]
			}
		case <-ticker.C:
			ticker.Stop()
			err := svc.processTaskWithMetrics(ctx, msgs)
			if err != nil {
				svc.logger.Error("unable to process kafka message", zap.Error(err))
				if portfolio.ErrorCode(err) == portfolio.ErrorCodeInternal {
					svc.mu.Lock()
					svc.cancel()
					svc.mu.Unlock()

					return
				}
			}
			ticker.Reset(svc.bucketWaitTime)
			// reset kafka message bucket.
			msgs = msgs[:0]
		}
	}
}

func (svc *Service) processMessages(
	ctx context.Context,
	msg portfolio.KafkaMsg,
) (data *dataPointMsg, err error) {
	dataPoint, err := svc.parseDatapoint(msg.Msg())
	if err != nil {
		svc.logger.Error("cannot parse message from Kafka message", zap.Error(err))
		if err = msg.Commit(ctx); err != nil {
			svc.logger.Error("unable to commit kafka message", zap.Error(err))

			return
		}

		return nil, nil
	}

	// convert message to db friendly struct.
	dpGranul := &portfolio.DataPointPerGranularity{
		Granularity: datapoint.GetGranularity(dataPoint.Timestamp),
		Data: &portfolio.DataPoint{
			UserID:    dataPoint.UserID,
			Currency:  dataPoint.Currency,
			Amount:    dataPoint.Amount,
			Timestamp: dataPoint.Timestamp,
		},
	}

	// skip unwanted granularity.
	if dpGranul.Granularity == "" || dpGranul.Granularity == portfolio.FifteenMinutesGranulation {
		if err = msg.Commit(ctx); err != nil {
			svc.logger.Error("unable to commit kafka message", zap.Error(err))

			return
		}

		return nil, nil
	}

	return &dataPointMsg{
		datapoint: dpGranul,
		kafkaMsg:  msg,
	}, nil
}

func (svc *Service) processTaskWithMetrics(
	ctx context.Context,
	msgs []*dataPointMsg,
) (err error) {
	if len(msgs) == 0 {
		return nil
	}

	span := sentry.StartSpan(
		ctx,
		"datapoint",
		sentry.TransactionName("data-point-consumer"),
	)

	stime := time.Now()
	defer func() {
		// populate histogram and counter metrics for data point insertion.
		labels := []string{
			"error", fmt.Sprintf("%t", err != nil),
			"consumer", svc.Name(),
		}
		svc.metricMsgDuration.With(labels...).Observe(time.Since(stime).Seconds())
		svc.metricMsgTotal.With(labels...).Add(float64(len(msgs)))
		if err != nil {
			hub := sentry.GetHubFromContext(ctx)
			hub.CaptureException(err)

			return
		}
	}()
	defer span.Finish()

	if err = svc.processTask(ctx, msgs); err != nil {
		// processTask only insert data to database and only internal error that can happen
		// then return without committing kafka message
		return
	}

	return err
}

//nolint:funlen
func (svc *Service) processTask(ctx context.Context, msgs []*dataPointMsg) error {
	dataPoints := make([]*portfolio.DataPointPerGranularity, 0, len(msgs))

	earliestTs := time.Now().Add(time.Hour * 24) // initiate the earliest timestamp with future timestamp
	var latestTs time.Time                       // initiate the latest timestamp with 0 timestamp
	processID := uuid.New().String()
	detailLogFields := []zapcore.Field{
		zap.String("process_id", processID),
	}
	logger := svc.logger.With(detailLogFields...)

	detailLogFields = append(detailLogFields,
		zap.Int("bucket_size", svc.bucketSize),
		zap.Int("total_messages", len(msgs)),
	)

	for _, msg := range msgs {
		dp := msg.datapoint.Data
		dpTs := dp.Timestamp
		// find the earliest timestamp by comparing message timestamp with current earliest timestamp
		if dpTs.Before(earliestTs) {
			earliestTs = dpTs
		}
		// find the latest timestamp by comparing message timestamp with current latest timestamp
		if dpTs.After(latestTs) {
			latestTs = dpTs
		}

		dataPoints = append(dataPoints, msg.datapoint)
	}

	detailLogFields = append(detailLogFields,
		zap.Time("earliest_dp_timestamp", earliestTs),
		zap.Time("latest_dp_timestamp", latestTs),
	)

	logger.Info("trying to do bulk insert with slow insert", detailLogFields...)
	tInsBulk := time.Now().UTC()
	if err := svc.portfolioRepo.UpsertDatapoint(ctx, dataPoints); err != nil {
		logger.Error("error on bulk insert with slow insert", zap.Error(err))

		return err
	}
	logger.Info("finish to do bulk insert with slow insert", zap.Duration("duration", time.Since(tInsBulk)))

	logger.Info("commit started")
	tAck := time.Now().UTC()
	for _, m := range msgs {
		if err := m.kafkaMsg.Commit(ctx); err != nil {
			logger.Error(
				"unable to commit kafka message",
				zap.Int("bucket_size", svc.bucketSize),
				zap.Error(err),
			)

			return err
		}
	}
	logger.Info("commit ended", zap.Duration("duration", time.Since(tAck)))

	return nil
}

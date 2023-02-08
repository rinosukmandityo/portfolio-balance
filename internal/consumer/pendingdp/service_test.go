package pendingdp_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/rinosukmandityo/portfolio-balance"
	"github.com/rinosukmandityo/portfolio-balance/internal/consumer/pendingdp"
	"github.com/rinosukmandityo/portfolio-balance/internal/kafka/schemas"
	"github.com/rinosukmandityo/portfolio-balance/internal/mock"
	"github.com/rinosukmandityo/portfolio-balance/internal/monitoring"
)

type int32Cnt struct {
	v int32
}

func newInt32Cnt(v int32) *int32Cnt {
	return &int32Cnt{
		v: v,
	}
}
func (cnt *int32Cnt) inc() {
	atomic.AddInt32(&cnt.v, 1)
}

func (cnt *int32Cnt) dec() {
	atomic.AddInt32(&cnt.v, -1)
}

func (cnt *int32Cnt) value() int32 {
	return atomic.LoadInt32(&cnt.v)
}

func TestService_PendingKafkaConsumer_Success(t *testing.T) {
	t.Parallel()

	apdDecimal := apd.New(0, 0)
	amount, _ := apdDecimal.SetFloat64(100.25)
	tx := &portfolio.DataPointMessage{
		UserID:    "8d03f824fd0848c68c7a4d7c786a4b4e",
		Currency:  "PHP",
		Timestamp: time.Date(2022, 9, 22, 6, 0, 0, 0, time.UTC),
		SentAt:    time.Now(),
		Amount:    amount,
	}

	var totalData int32 = 798_020
	var bucketSize int32 = 100_000
	waitTime := time.Second
	expTxWrite := newInt32Cnt(0)
	totalBulkInsert := (totalData + 1) / bucketSize
	ackCount := newInt32Cnt(0)
	totalInsertCount := newInt32Cnt(0)
	txWrite := newInt32Cnt(totalData)
	expTotalInsert := newInt32Cnt(totalBulkInsert)
	expAckCount := newInt32Cnt(totalData)
	ctx, cancel := context.WithCancel(context.Background())
	logger := zap.NewNop()

	portfolioRepo := &mock.RepositoryMock{
		UpsertDatapointFunc: func(ctx context.Context, data []*portfolio.DataPointPerGranularity) error {
			totalInsertCount.inc()

			return nil
		},
	}

	svc, err := pendingdp.NewService(
		logger,
		newKafkaReaderFunc(totalData, ackCount, txWrite, tx, cancel),
		portfolioRepo,
		pendingdp.WithConsumersCount(1),
		pendingdp.WithBucketSize(int(bucketSize)),
		pendingdp.WithBucketWaitTime(waitTime),
		pendingdp.WithMsgDurationMetric(monitoring.ConsumeDurationSeconds),
		pendingdp.WithMsgTotalMetric(monitoring.ConsumeTotal),
	)

	require.NoError(t, err)
	svc.Start(ctx)

	require.Equal(t, expAckCount.value(), ackCount.value())
	require.Equal(t, expTxWrite.value(), txWrite.value())
	require.GreaterOrEqual(t, totalInsertCount.value(), expTotalInsert.value())
}

func TestService_PendingKafkaConsumer_Failed(t *testing.T) {
	t.Parallel()

	apdDecimal := apd.New(0, 0)
	amount, _ := apdDecimal.SetFloat64(100.25)
	tx := &portfolio.DataPointMessage{
		UserID:    "8d03f824fd0848c68c7a4d7c786a4b4e",
		Currency:  "PHP",
		Timestamp: time.Date(2022, 9, 22, 6, 0, 0, 0, time.UTC),
		SentAt:    time.Now(),
		Amount:    amount,
	}

	var totalData int32 = 10_000
	var bucketSize int32 = 1_000
	waitTime := time.Second
	expTxWrite := newInt32Cnt(0)
	ackCount := newInt32Cnt(0)
	totalInsertCount := newInt32Cnt(0)
	txWrite := newInt32Cnt(totalData)
	expAckCount := newInt32Cnt(0)
	ctx, cancel := context.WithCancel(context.Background())
	logger := zap.NewNop()

	portfolioRepo := &mock.RepositoryMock{
		UpsertDatapointFunc: func(ctx context.Context, data []*portfolio.DataPointPerGranularity) error {
			totalInsertCount.inc()

			return portfolio.Error{
				Code:    portfolio.ErrorCodeInternal,
				Message: "some errors",
			}
		},
	}

	svc, err := pendingdp.NewService(
		logger,
		newKafkaReaderFunc(totalData, ackCount, txWrite, tx, cancel),
		portfolioRepo,
		pendingdp.WithConsumersCount(1),
		pendingdp.WithBucketSize(int(bucketSize)),
		pendingdp.WithBucketWaitTime(waitTime),
		pendingdp.WithMsgDurationMetric(monitoring.ConsumeDurationSeconds),
		pendingdp.WithMsgTotalMetric(monitoring.ConsumeTotal),
	)

	require.NoError(t, err)
	svc.Start(ctx)

	require.Equal(t, expAckCount.value(), ackCount.value())
	require.Greater(t, txWrite.value(), expTxWrite.value())
}

func newKafkaReaderFunc(
	totalData int32,
	ackCount,
	txWrite *int32Cnt,
	tx *portfolio.DataPointMessage,
	cancel context.CancelFunc,
) portfolio.KafkaReaderFn {
	codec, err := goavro.NewCodec(schemas.DataPointSchema)
	if err != nil {
		panic(err)
	}

	kafkaRdr := &mock.KafkaReaderMock{
		FetchMessageFunc: func(ctx context.Context) (portfolio.KafkaMsg, error) {
			if txWrite.value() <= 0 {
				for {
					if ackCount.value() < totalData {
						time.Sleep(time.Millisecond * 200)
					} else {
						break
					}
				}

				return nil, fmt.Errorf("data exhausted")
			} else {
				txWrite.dec()
			}

			return &mock.KafkaMsgMock{
				CommitFunc: func(ctx context.Context) error {
					if ackCount.value() >= totalData {
						cancel()
					} else {
						ackCount.inc()
					}

					return nil
				},
				MsgFunc: func() []byte {
					b, err := codec.BinaryFromNative(nil, map[string]interface{}{
						"user_id":  tx.UserID,
						"currency": tx.Currency,
						"period":   tx.Timestamp,
						"amount":   tx.Amount.String(),
						"sent_at":  tx.SentAt,
					})
					if err != nil {
						panic(err)
					}

					return b
				},
				PartitionFunc: func() int {
					return 0
				},
				OffsetFunc: func() int64 {
					return 0
				},
			}, nil
		},
		CloseFunc: func() error {
			return nil
		},
	}

	return func() (portfolio.KafkaReader, error) {
		return kafkaRdr, nil
	}
}

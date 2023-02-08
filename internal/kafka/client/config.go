package client

import (
	"errors"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/generic"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	// following is the Bytes Calculation:
	// user_id: 64 bytes
	// currency: 2 bytes
	// period: 4 bytes
	// amount: 4 bytes
	// sent_at: 4 bytes
	// 1 total data for 1 user for 1 data point = 64 + 2 + 4 + 4 + 4 = 78 B.
	defaultMinBytes = 78
	defaultMaxBytes = 2e6 // 2MB
	defaultMaxWait  = time.Second * 3
)

// ErrNoBroker return when config doesn't have broker.
var ErrNoBroker = errors.New("invalid config: no broker set up")

// ReaderConfig reader config.
type ReaderConfig struct {
	Logger         *zap.Logger
	Topic          string
	Brokers        []string
	StartOffset    int64
	SpecificOffset int64
	Partition      int
	GroupID        string
	MinBytes       int
	MaxBytes       int
	MaxWait        time.Duration
	MetricFetchMsg metrics.Histogram
}

func (r *ReaderConfig) validateConfig() {
	// based on library documentation, if non-zero, it must be set to one of FirstOffset or LastOffset.
	// for our case if the StartOffset is 0 that means it is not set and the default value is LastOffset.
	if r.StartOffset == 0 {
		r.StartOffset = kafka.LastOffset
	}
	// if the StartOffset is more than 0 that means we want to start from SpecificOffset.
	if r.StartOffset > 0 {
		r.SpecificOffset = r.StartOffset
	}
	if r.MinBytes == 0 {
		r.MinBytes = defaultMinBytes
	}
	if r.MaxBytes == 0 {
		r.MaxBytes = defaultMaxBytes
	}
	if r.MaxWait == 0 {
		r.MaxWait = defaultMaxWait
	}
	if r.MetricFetchMsg == nil {
		r.MetricFetchMsg = generic.NewHistogram("nop_fetch_message", 1)
	}
}

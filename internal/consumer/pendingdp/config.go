package pendingdp

import (
	"time"

	"github.com/go-kit/kit/metrics"
	"golang.org/x/time/rate"
)

// Option is an option for Service.
type Option func(*Service)

// WithConsumersCount set total number of consumers in a consumer group.
func WithConsumersCount(cnt int) Option {
	return func(svc *Service) {
		svc.consumersCount = cnt
	}
}

// WithBucketSize set size of data point bucket.
func WithBucketSize(size int) Option {
	return func(svc *Service) {
		svc.bucketSize = size
	}
}

// WithBucketWaitTime set wait time for populating data point bucket.
func WithBucketWaitTime(waitTime time.Duration) Option {
	return func(svc *Service) {
		svc.bucketWaitTime = waitTime
	}
}

// WithMsgDurationMetric configures message consumed duration metric.
func WithMsgDurationMetric(metric metrics.Histogram) Option {
	return func(svc *Service) {
		svc.metricMsgDuration = metric
	}
}

// WithMsgTotalMetric configures total message metric.
func WithMsgTotalMetric(metric metrics.Counter) Option {
	return func(svc *Service) {
		svc.metricMsgTotal = metric
	}
}

// WithTimestampAdjustment adds gap to every data timestamp read from kafka.
func WithTimestampAdjustment(gap time.Duration) Option {
	return func(svc *Service) {
		svc.timestampAdjustment = gap
	}
}

// WithMessageRateLimit configures message consumed duration metric.
func WithMessageRateLimit(rps float64) Option {
	return func(svc *Service) {
		if rps < 0 {
			svc.messageRPS = rate.Inf
		} else {
			svc.messageRPS = rate.Limit(rps)
		}
	}
}

package config

import "time"

// Kafka stores Kafka config.
type Kafka struct {
	User        string        `mapstructure:"user"`
	Password    string        `mapstructure:"password"`
	Brokers     []string      `mapstructure:"brokers"`
	Group       string        `mapstructure:"group"`
	Topic       string        `mapstructure:"topic"`
	StartOffset int64         `mapstructure:"start_offset"`
	MinBytes    int           `mapstructure:"min_bytes"`
	MaxBytes    int           `mapstructure:"max_bytes"`
	MaxWait     time.Duration `mapstructure:"max_wait"`
}

// Sentry stores Sentry config.
type Sentry struct {
	Enabled          bool    `mapstructure:"enabled"`
	DSN              string  `mapstructure:"dsn"`
	TracesSampleRate float64 `mapstructure:"traces_sample_rate"`
}

type DataPointConsumer struct {
	WorkersCount         int           `mapstructure:"workers_count"`
	BucketSize           int           `mapstructure:"bucket_size"`
	BucketWaitTime       time.Duration `mapstructure:"bucket_wait_time"`
	MessageRPS           float64       `mapstructure:"message_rps"`
	MessageBurst         int           `mapstructure:"message_burst"`
	TimeAdjustment       time.Duration `mapstructure:"data_point_timestamp" default:"15m"`
	InsertPendingEnabled bool          `mapstructure:"insert_pending_enabled" default:"true"`
}

type PendingKafkaConsumer struct {
	WorkersCount   int           `mapstructure:"workers_count"`
	BucketSize     int           `mapstructure:"bucket_size" default:"1000"`
	BucketWaitTime time.Duration `mapstructure:"bucket_wait_time" default:"3s"`
	BucketRPS      float64       `mapstructure:"bucket_rps"`
	TimeAdjustment time.Duration `mapstructure:"time_adjustment" default:"15m"`
}

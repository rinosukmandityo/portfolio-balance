package main

import (
	"context"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/rinosukmandityo/portfolio-balance/internal/config"
	"github.com/rinosukmandityo/portfolio-balance/internal/consumer/pendingdp"
	"github.com/rinosukmandityo/portfolio-balance/internal/kafka"
	"github.com/rinosukmandityo/portfolio-balance/internal/monitoring"
	"github.com/rinosukmandityo/portfolio-balance/internal/postgres"
)

type appConfiguration struct {
	// fill with common application settings

	// API contains settings for api web-server (example).
	// by default would listen on tcp://:8080.
	Env                  string                      `mapstructure:"env"`
	Debug                bool                        `mapstructure:"debug"`
	PostgresDSN          string                      `mapstructure:"postgres_dsn"`
	PostgresConnLimit    int                         `mapstructure:"postgres_conn_limit"`
	Kafka                config.Kafka                `mapstructure:"kafka"`
	PendingKafkaConsumer config.PendingKafkaConsumer `mapstructure:"pending_kafka_consumer"`
}

func main() {
	var appCfg appConfiguration
	logger, _ := zap.NewProduction()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.SetConfigName("app_config")
	viper.ReadInConfig()
	err := viper.Unmarshal(&appCfg)
	if err != nil {
		zap.L().Fatal("could not prepare config", zap.Error(err))
	}

	pgclient := postgres.NewClient(
		logger,
		postgres.WithMaxConnections(appCfg.PostgresConnLimit),
		postgres.WithLatencyMetric(monitoring.StorageDurationSeconds),
	)

	if err = pgclient.Open(appCfg.PostgresDSN); err != nil {
		zap.L().Fatal("could not open database", zap.Error(err))
	}
	defer pgclient.Close()

	repo := postgres.NewPortfolioStorage(pgclient)
	// initialize kafka reader func to create a new kafka consumer.
	kafkaReaderFunc := kafka.NewKafkaReaderFunc(
		logger.With(zap.String("service", "kafka_pending_reader")),
		appCfg.Kafka,
		monitoring.KafkaFetchMessageDurationSeconds,
		monitoring.KafkaAckMessageDurationSeconds,
	)

	kafkaPendingSvc, err := pendingdp.NewService(
		logger.With(zap.String("service", "kafka_pending_consumer")),
		kafkaReaderFunc,
		repo,
		pendingdp.WithConsumersCount(appCfg.PendingKafkaConsumer.WorkersCount),
		pendingdp.WithBucketSize(appCfg.PendingKafkaConsumer.BucketSize),
		pendingdp.WithBucketWaitTime(appCfg.PendingKafkaConsumer.BucketWaitTime),
		pendingdp.WithMsgDurationMetric(monitoring.ConsumeDurationSeconds),
		pendingdp.WithMsgTotalMetric(monitoring.ConsumeTotal),
		pendingdp.WithTimestampAdjustment(appCfg.PendingKafkaConsumer.TimeAdjustment),
		pendingdp.WithMessageRateLimit(appCfg.PendingKafkaConsumer.BucketRPS),
	)
	if err != nil {
		zap.L().Fatal("could not create kafka consumer group", zap.Error(err))
	}

	kafkaPendingSvc.Start(ctx)
}

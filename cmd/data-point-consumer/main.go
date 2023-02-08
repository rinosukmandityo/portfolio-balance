package main

import (
	"context"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/rinosukmandityo/portfolio-balance/internal/config"
	"github.com/rinosukmandityo/portfolio-balance/internal/consumer/datapoint"
	"github.com/rinosukmandityo/portfolio-balance/internal/kafka"
	"github.com/rinosukmandityo/portfolio-balance/internal/kafka/client"
	"github.com/rinosukmandityo/portfolio-balance/internal/monitoring"
	"github.com/rinosukmandityo/portfolio-balance/internal/postgres"
	st "github.com/rinosukmandityo/portfolio-balance/internal/sentry"
)

type appConfiguration struct {
	// fill with common application settings

	// API contains settings for api web-server (example).
	// by default would listen on tcp://:8080.
	Env                string                   `mapstructure:"env"`
	Debug              bool                     `mapstructure:"debug"`
	PostgresDSN        string                   `mapstructure:"postgres_dsn"`
	PostgresConnLimit  int                      `mapstructure:"postgres_conn_limit"`
	Sentry             config.Sentry            `mapstructure:"sentry"`
	Kafka              config.Kafka             `mapstructure:"kafka"`
	PendingKafkaTopic  string                   `mapstructure:"pending_kafka_topic"`
	DataPointConsumer  config.DataPointConsumer `mapstructure:"data_point_consumer"`
	BulkInsertFallback string                   `mapstructure:"bulkinsert_fallback" default:"raise_error"`
}

// version is the service version from git tag.
var version = "dev"

func main() {
	var appCfg appConfiguration

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger, _ := zap.NewProduction()

	viper.AddConfigPath(".")
	viper.SetConfigName("app_config")
	viper.SetConfigType("yaml")
	viper.ReadInConfig()
	err := viper.Unmarshal(&appCfg)
	if err != nil {
		zap.L().Fatal("could not prepare config", zap.Error(err))
	}

	// Sentry
	sentryConfig := appCfg.Sentry
	if sentryConfig.Enabled {
		sentrySyncTransport := sentry.NewHTTPSyncTransport()
		sentrySyncTransport.Timeout = time.Second * 3

		err = sentry.Init(sentry.ClientOptions{
			Dsn:              sentryConfig.DSN,
			Transport:        sentrySyncTransport,
			TracesSampleRate: sentryConfig.TracesSampleRate,
			Environment:      appCfg.Env,
			Release:          version,
			Debug:            appCfg.Debug,
			BeforeSend:       st.StripSensitiveData,
		})
		if err != nil {
			zap.L().Fatal("could not initialize sentry", zap.Error(err))
		}

		sentry.ConfigureScope(func(scope *sentry.Scope) {
			scope.AddEventProcessor(st.StripSensitiveData)
		})
		defer sentry.Flush(2 * time.Second)
		defer sentry.Recover()
	}

	// register postgres client.
	pgclient := postgres.NewClient(
		logger,
		postgres.WithMaxConnections(appCfg.PostgresConnLimit),
		postgres.WithLatencyMetric(monitoring.StorageDurationSeconds),
		postgres.WithBulkInsertMetric(monitoring.StorageBulkInsertBlock),
	)
	if err = pgclient.Open(appCfg.PostgresDSN); err != nil {
		zap.L().Fatal("could not open database", zap.Error(err))
	}
	defer pgclient.Close()

	portfolioRepo := postgres.NewPortfolioStorage(
		pgclient,
		postgres.WithBulkInsertFallback(appCfg.BulkInsertFallback),
	)

	// initialize kafka reader func to create a new kafka consumer.
	kafkaReaderFunc := kafka.NewKafkaReaderFunc(
		logger,
		appCfg.Kafka,
		monitoring.KafkaFetchMessageDurationSeconds,
		monitoring.KafkaAckMessageDurationSeconds,
	)

	var pendingWriter client.ClientWriter
	pendingKafkaCfg := appCfg.Kafka
	pendingKafkaCfg.Topic = appCfg.PendingKafkaTopic
	pendingWriter, err = client.NewWriter(
		logger,
		pendingKafkaCfg,
		client.WithMsgDurationMetric(monitoring.KafkaWriteMessageDurationSeconds),
		client.WithMsgTotalMetric(monitoring.KafkaWriteMessagesTotal),
	)
	if err != nil {
		zap.L().Fatal("could not prepare kafka writer", zap.Error(err))
	}

	// initialize kafka consumer group.
	svc, err := datapoint.NewService(
		logger,
		kafkaReaderFunc,
		portfolioRepo,
		pendingWriter,
		datapoint.WithConsumersCount(appCfg.DataPointConsumer.WorkersCount),
		datapoint.WithBucketSize(appCfg.DataPointConsumer.BucketSize),
		datapoint.WithBucketWaitTime(appCfg.DataPointConsumer.BucketWaitTime),
		datapoint.WithMsgDurationMetric(monitoring.ConsumeDurationSeconds),
		datapoint.WithMsgTotalMetric(monitoring.ConsumeTotal),
		datapoint.WithMsgLagDurationMetric(monitoring.ConsumeLagSeconds),
		datapoint.WithTimestampAdjustment(appCfg.DataPointConsumer.TimeAdjustment),
		datapoint.WithMessageRateLimit(appCfg.DataPointConsumer.MessageRPS, appCfg.DataPointConsumer.MessageBurst),
		datapoint.WithInsertPending(appCfg.DataPointConsumer.InsertPendingEnabled),
	)
	if err != nil {
		zap.L().Fatal("could not create kafka consumer group", zap.Error(err))
	}
	svc.Start(ctx)
}

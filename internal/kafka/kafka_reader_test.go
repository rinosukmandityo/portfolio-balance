package kafka_test

import (
	"context"
	"net"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/rinosukmandityo/portfolio-balance/internal/kafka"
	"github.com/rinosukmandityo/portfolio-balance/internal/kafka/client"
	"github.com/rinosukmandityo/portfolio-balance/internal/monitoring"
)

const (
	username = "adminscram"
	password = "admin-secret-512"
	broker   = "127.0.0.1:9093"
	topic    = "transaction"
)

func TestFetchMessage(t *testing.T) {
	expMsg := [][]byte{
		[]byte("1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
		[]byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."),
	}

	err := kafkaProducer(context.Background(), t, expMsg)
	require.NoError(t, err)

	logger, _ := zap.NewDevelopment()
	dialer, err := client.NewDialer(username, password, time.Second*5)
	require.NoError(t, err)

	reader, err := client.NewReader(
		dialer,
		&client.ReaderConfig{
			Logger:         logger,
			Topic:          topic,
			Brokers:        []string{broker},
			MetricFetchMsg: monitoring.KafkaFetchMessageDurationSeconds,
		},
	)
	require.NoError(t, err)
	defer reader.Close()

	byteReader, err := kafka.NewKafkaReader(reader, logger, monitoring.KafkaAckMessageDurationSeconds)
	require.NoError(t, err)
	kafkaMsg, err := byteReader.FetchMessage(context.Background())
	require.NoError(t, err)
	require.Equal(t, expMsg[0], kafkaMsg.Msg())
	kafkaMsg, err = byteReader.FetchMessage(context.Background())
	require.NoError(t, err)
	require.Equal(t, expMsg[1], kafkaMsg.Msg())
}

func kafkaProducer(ctx context.Context, t *testing.T, msgs [][]byte) error {
	t.Helper()

	mechanism, err := scram.Mechanism(scram.SHA512, username, password)
	require.NoError(t, err)

	// initialize the writer with the broker addresses, and the topic
	w := &kafkago.Writer{
		Transport: &kafkago.Transport{
			SASL: mechanism,
			Dial: (&net.Dialer{
				Timeout: time.Second * 10,
			}).DialContext,
		},
		Addr:         kafkago.TCP(broker),
		Topic:        topic,
		RequiredAcks: kafkago.RequireAll,
		Balancer:     &kafkago.LeastBytes{},
	}
	defer w.Close()

	counter := 0
	for _, msg := range msgs {
		err = w.WriteMessages(ctx, kafkago.Message{
			Value: msg,
		})
		if err != nil {
			return err
		}
		counter++
	}
	require.Equal(t, len(msgs), counter)

	return nil
}

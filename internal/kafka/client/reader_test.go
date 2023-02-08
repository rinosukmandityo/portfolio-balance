package client_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rinosukmandityo/portfolio-balance/internal/kafka/client"
)

const (
	username = "adminscram"
	password = "admin-secret-512"
	broker   = "127.0.0.1:9093"
	topic    = "transaction"
)

func TestDialerAndReader(t *testing.T) {
	kafkaDialer, err := client.NewDialer(username, password, time.Second)
	require.NoError(t, err)

	reader, err := client.NewReader(
		kafkaDialer,
		&client.ReaderConfig{
			Brokers: []string{broker},
			Topic:   topic,
		},
	)
	require.NoError(t, err)
	reader.Close()
}

package postgres

import (
	"fmt"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/generic"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	// pq registers itself for the usage.
	_ "github.com/lib/pq"
)

// Option configures the client.
type Option func(c *Client)

// Client represents a client to the underlying PostgreSQL data store.
type Client struct {
	db          *sqlx.DB
	logger      *zap.Logger
	latency     metrics.Histogram
	bulkCounter metrics.Counter
	connections int
}

// NewClient constructs a new client instance.
func NewClient(logger *zap.Logger, options ...Option) *Client {
	c := &Client{
		logger:      logger,
		latency:     generic.NewHistogram("nop_latency", 1),
		bulkCounter: generic.NewCounter("nop_counter"),
	}

	for _, option := range options {
		option(c)
	}

	return c
}

// Open establishes connection to PostgreSQL database.
func (c *Client) Open(dsn string) error {
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		c.logger.Error("connection failed", zap.Error(err))

		return err
	}

	c.db = db
	c.db.SetMaxIdleConns(c.connections)
	c.db.SetMaxOpenConns(c.connections)
	return nil
}

// Close closes connection.
func (c *Client) Close() error {
	return c.db.Close()
}

// updateMetric update latency metrics.
func (c *Client) updateMetric(method string, t time.Time, err *error) {
	labels := []string{
		"method", method,
		"error", fmt.Sprintf("%t", *err != nil),
	}

	c.latency.With(labels...).Observe(time.Since(t).Seconds())
}

// WithLatencyMetric configures latency metric.
func WithLatencyMetric(histogram metrics.Histogram) Option {
	return func(c *Client) {
		c.latency = histogram
	}
}

// WithBulkInsertMetric configures bulk insert metric.
func WithBulkInsertMetric(counter metrics.Counter) Option {
	return func(c *Client) {
		c.bulkCounter = counter
	}
}

// WithMaxConnections configures max opened and idle connections.
func WithMaxConnections(connections int) Option {
	return func(c *Client) {
		c.connections = connections
	}
}

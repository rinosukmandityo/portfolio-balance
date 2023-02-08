package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/lib/pq"
	"go.opencensus.io/trace"
	"go.uber.org/zap"

	"github.com/rinosukmandityo/portfolio-balance"
)

const (
	// a postgres db level error code, ref: https://www.postgresql.org/docs/current/errcodes-appendix.html.
	errCodeUniqueViolation = "23505"
	tableName              = "portfolio_balance"
)

type dataPointModel struct {
	UserID      string                `db:"user_id"`
	Currency    string                `db:"currency"`
	Amount      *apd.Decimal          `db:"amount"`
	Granularity portfolio.Granulation `db:"granularity"`
	IssuedAt    time.Time             `db:"issued_at"`
	CreatedAt   time.Time             `db:"created_at"`
}

// PortfolioStorage implements portfolio.Repository.
type PortfolioStorage struct {
	*Client
	bulkFallbackMethod BulkInsertMethod
}

type BulkInsertMethod string

const (
	// BulkInsertUpdate is using upsert - we overwrite old data on database with new data from kafka.
	BulkInsertUpdate BulkInsertMethod = "update"
	// BulkInsertRaiseError is just returning the error when data is duplicate.
	BulkInsertRaiseError BulkInsertMethod = "raise_error"
)

type StorageOption func(storage *PortfolioStorage)

// WithBulkInsertFallback configures bulk insert method.
func WithBulkInsertFallback(method string) StorageOption {
	return func(c *PortfolioStorage) {
		mtd := BulkInsertMethod(method)
		switch mtd {
		case BulkInsertUpdate, BulkInsertRaiseError:
			c.bulkFallbackMethod = mtd
		default:
			panic(fmt.Errorf("unknown method for configuration WithBulkInsertFallback: %s", method))
		}
	}
}

// NewPortfolioStorage creates a new PortfolioStorage instance.
func NewPortfolioStorage(client *Client, options ...StorageOption) *PortfolioStorage {
	p := &PortfolioStorage{
		Client:             client,
		bulkFallbackMethod: BulkInsertRaiseError,
	}

	for _, option := range options {
		option(p)
	}

	return p
}

// InsertBulk inserts data-point in a bulk.
// Raise ErrorCodeInternal in case of any errors.
// Fallback to slow insert if duplication error happens.
func (p *PortfolioStorage) InsertBulk(ctx context.Context, data []*portfolio.DataPointPerGranularity) (err error) {
	const method = "PortfolioStorage.InsertBulk"
	var path = "fast"
	ctx, span := trace.StartSpan(ctx, method)
	defer func(tm time.Time) {
		span.End()
		p.updateMetric(method, tm, &err)

		labels := []string{
			"path", path,
		}

		p.bulkCounter.With(labels...).Add(1)
	}(time.Now())

	err = p.insertFast(ctx, data)
	// if there is duplicate data then fallback to upsert
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == errCodeUniqueViolation {
		p.logger.Error("duplication error on bulk insert",
			zap.Error(err),
			zap.String("detail", pqErr.Detail),
			zap.String("where", pqErr.Where),
		)

		path = "slow"

		switch p.bulkFallbackMethod {
		case BulkInsertUpdate:
			err = p.upsertSlow(ctx, data)
		case BulkInsertRaiseError:
			err = portfolio.ErrorDuplicateDataPoint
		}
	}

	return err
}

// UpsertDatapoint do upsert on some datapoints.
// This is slow operation.
func (p *PortfolioStorage) UpsertDatapoint(ctx context.Context, data []*portfolio.DataPointPerGranularity) (err error) {
	const method = "PortfolioStorage.UpsertDatapoint"
	ctx, span := trace.StartSpan(ctx, method)
	defer func(tm time.Time) {
		span.End()
		p.updateMetric(method, tm, &err)
	}(time.Now())

	return p.upsertSlow(ctx, data)
}

func (p *PortfolioStorage) upsertSlow(ctx context.Context, data []*portfolio.DataPointPerGranularity) (err error) {
	const method = "PortfolioStorage.upsertSlow"
	ctx, span := trace.StartSpan(ctx, method)
	defer func(tm time.Time) {
		span.End()
		p.updateMetric(method, tm, &err)
	}(time.Now())

	return p.insertSlowQuery(ctx, data, upsertPortfolioBalance)
}

func (p *PortfolioStorage) insertSlowQuery(
	ctx context.Context,
	data []*portfolio.DataPointPerGranularity,
	query string,
) (err error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err)
	}
	defer func() {
		if err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				p.logger.Error("[insertSlowQuery] error on rollback", zap.Error(err))

				return
			}
		}
	}()

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, dp := range data {
		_, err = stmt.Exec(
			dp.Data.UserID,
			strings.ToUpper(dp.Data.Currency),
			dp.Data.Amount,
			dp.Granularity,
			dp.Data.Timestamp)
		if err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return internalError(err)
	}

	return nil
}

//nolint:funlen
func (p *PortfolioStorage) insertFast(ctx context.Context, data []*portfolio.DataPointPerGranularity) (err error) {
	const method = "PortfolioStorage.insertFast"
	ctx, span := trace.StartSpan(ctx, method)
	defer func(tm time.Time) {
		span.End()
		p.updateMetric(method, tm, &err)
	}(time.Now())

	tx, err := p.db.BeginTxx(ctx, nil)
	if err != nil {
		return internalError(err)
	}

	defer func() {
		if err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				p.logger.Error("[insertFast] error on rollback", zap.Error(err))

				return
			}
		}
	}()

	stmt, err := tx.PrepareContext(ctx, pq.CopyIn(tableName,
		"user_id",
		"currency",
		"amount",
		"granularity",
		"issued_at"))
	if err != nil {
		return internalError(err)
	}
	defer func() {
		if errStmt := stmt.Close(); errStmt != nil {
			p.logger.Error("error on closing prepared statement", zap.Error(err))

			return
		}
	}()

	for _, dp := range data {
		_, err = stmt.ExecContext(
			ctx,
			dp.Data.UserID,
			strings.ToUpper(dp.Data.Currency),
			dp.Data.Amount,
			dp.Granularity,
			dp.Data.Timestamp)
		if err != nil {
			return internalError(err)
		}
	}

	if _, err = stmt.ExecContext(ctx); err != nil {
		return internalError(err)
	}

	if err = stmt.Close(); err != nil {
		return internalError(err)
	}

	if err = tx.Commit(); err != nil {
		return internalError(err)
	}

	return nil
}

func makeModelFromDataPoint(dataPoint *portfolio.DataPoint, granularity portfolio.Granulation) *dataPointModel {
	return &dataPointModel{
		UserID:      dataPoint.UserID,
		Currency:    dataPoint.Currency,
		Amount:      dataPoint.Amount,
		Granularity: granularity,
		IssuedAt:    dataPoint.Timestamp,
	}
}

func makeDataPointListFromModels(models []*dataPointModel) []*portfolio.DataPoint {
	res := make([]*portfolio.DataPoint, 0, len(models))
	for _, model := range models {
		res = append(res, makeDataPointFromModel(model))
	}

	return res
}

func makeDataPointFromModel(model *dataPointModel) *portfolio.DataPoint {
	amount := model.Amount

	return &portfolio.DataPoint{
		UserID:    model.UserID,
		Currency:  model.Currency,
		Amount:    amount,
		Timestamp: model.IssuedAt,
	}
}

func internalError(err error) error {
	return portfolio.Error{
		Code:    portfolio.ErrorCodeInternal,
		Message: "Error perform SQL",
		Inner:   err,
	}
}

package portfolio

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/apd"
)

type (
	// Resolution defines custom data type for resolution.
	Resolution string
	// Granulation is a granularity.
	Granulation string
)

const (
	// ResolutionHour is an hourly resolution.
	ResolutionHour Resolution = "1h"
	// ResolutionDay is a daily resolution.
	ResolutionDay Resolution = "1d"
	// ResolutionWeek is a weekly resolution.
	ResolutionWeek Resolution = "1w"
	// ResolutionMonth is a monthly resolution.
	ResolutionMonth Resolution = "1m"
	// ResolutionQuarter is a quarter resolution.
	ResolutionQuarter Resolution = "3m"
	// ResolutionHalfYear is a half-year resolution.
	ResolutionHalfYear Resolution = "6m"
	// ResolutionYear is a yearly resolution.
	ResolutionYear Resolution = "1y"
	// ResolutionAll is a resolution that means all available data.
	ResolutionAll Resolution = "all"
)

const (
	// FifteenMinutesGranulation means 15 minutes granularity.
	FifteenMinutesGranulation Granulation = "15min"
	// HourGranulation means one hour granularity.
	HourGranulation Granulation = "1hour"
	// SixHoursGranulation means 6 hours granularity.
	SixHoursGranulation Granulation = "6hours"
	// TwelveHoursGranulation means 12 hours granularity.
	TwelveHoursGranulation Granulation = "12hours"
	// DayGranulation means 24 hours granularity.
	DayGranulation Granulation = "1day"
	// WeekGranulation means 1 week granularity.
	WeekGranulation Granulation = "1week"
)

const (
	// DailyResolutionLimit is number of datapoint required to show single day data with 15 minutes granulation.
	DailyResolutionLimit = 96
	// WeeklyResolutionLimit is number of datapoint required to show single week data with 1 hour granulation.
	WeeklyResolutionLimit = 168
	// MonthlyResolutionLimit is number of datapoint required to show single month data with 6 hour granulation.
	MonthlyResolutionLimit = 120
	// QuarterResolutionLimit is number of datapoint required to show single quarterly data with 12 hour granulation.
	QuarterResolutionLimit = 180
	// HalfYearlyResolutionLimit is number of datapoint required to show half year data with 1 day granulation.
	HalfYearlyResolutionLimit = 180
	// YearlyResolutionLimit is number of datapoint required to show single year data with 1 day granulation.
	YearlyResolutionLimit = 365
	// AllResolutionLimit is number of datapoint required to show more than 1 year data with 1 week granulation.
	AllResolutionLimit = 520
)

// Truncate will truncate time to nearby past datapoint that match with the resolution.
func (r Resolution) Truncate(t time.Time) (time.Time, error) {
	t = t.UTC()
	granul, err := r.Granularity()
	if err != nil {
		return time.Time{}, err
	}
	switch granul {
	case FifteenMinutesGranulation:
		return t.Truncate(15 * time.Minute), nil
	case HourGranulation:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()), nil
	case SixHoursGranulation:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour()/6*6, 0, 0, 0, t.Location()), nil
	case TwelveHoursGranulation:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour()/12*12, 0, 0, 0, t.Location()), nil
	case DayGranulation:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), nil
	case WeekGranulation:
		move := time.Duration(0)
		const oneDayBack = -24 * time.Hour
		diffDay := int(t.Weekday()) - int(time.Monday)
		if diffDay < 0 {
			move = time.Duration(7+diffDay) * oneDayBack
		} else if diffDay > 0 {
			move = time.Duration(diffDay) * oneDayBack
		}
		iter := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		return iter.Add(move), nil
	default:
		return time.Time{}, fmt.Errorf("unexpected resolution %s", r)
	}
}

// SubtractTime subtracts resolution period from given time.
func (r Resolution) SubtractTime(t time.Time) (time.Time, error) {
	switch r {
	case ResolutionDay:
		return t.Add(-24 * time.Hour), nil
	case ResolutionWeek:
		return t.AddDate(0, 0, -7), nil
	case ResolutionMonth:
		return t.AddDate(0, -1, 0), nil
	case ResolutionQuarter:
		return t.AddDate(0, -3, 0), nil
	case ResolutionHalfYear:
		return t.AddDate(0, -6, 0), nil
	case ResolutionYear:
		return t.AddDate(-1, 0, 0), nil
	case ResolutionAll:
		// set to start of unix time because we allow all data point to be returned
		return time.Unix(0, 0), nil
	default:
		return time.Time{}, fmt.Errorf("unexpected resolution %s", r)
	}
}

// Granularity returns either granularity associated with resolution or error.
func (r Resolution) Granularity() (Granulation, error) {
	switch r {
	case ResolutionDay:
		return FifteenMinutesGranulation, nil
	case ResolutionWeek:
		return HourGranulation, nil
	case ResolutionMonth:
		return SixHoursGranulation, nil
	case ResolutionQuarter:
		return TwelveHoursGranulation, nil
	case ResolutionHalfYear, ResolutionYear:
		return DayGranulation, nil
	case ResolutionAll:
		return WeekGranulation, nil
	}

	return "", fmt.Errorf("unexpected resolution %s", r)
}

// GranularityDuration returns either granularity duration associated with resolution or error.
func (r Resolution) GranularityDuration() (time.Duration, error) {
	granul, err := r.Granularity()
	if err != nil {
		return time.Duration(0), err
	}

	switch granul {
	case FifteenMinutesGranulation:
		return 15 * time.Minute, nil
	case HourGranulation:
		return time.Hour, nil
	case SixHoursGranulation:
		return 6 * time.Hour, nil
	case TwelveHoursGranulation:
		return 12 * time.Hour, nil
	case DayGranulation:
		return 24 * time.Hour, nil
	case WeekGranulation:
		return 7 * 24 * time.Hour, nil
	}

	return time.Duration(0), fmt.Errorf("unexpected resolution %s", r)
}

// ResolutionLimit returns number of datapoint required for associated resolution.
func (r Resolution) ResolutionLimit() (int, error) {
	switch r {
	case ResolutionDay:
		return DailyResolutionLimit, nil
	case ResolutionWeek:
		return WeeklyResolutionLimit, nil
	case ResolutionMonth:
		return MonthlyResolutionLimit, nil
	case ResolutionQuarter:
		return QuarterResolutionLimit, nil
	case ResolutionHalfYear:
		return HalfYearlyResolutionLimit, nil
	case ResolutionYear:
		return YearlyResolutionLimit, nil
	case ResolutionAll:
		return AllResolutionLimit, nil
	}

	return -1, fmt.Errorf("unexpected resolution %s", r)
}

// DataPoint holds portfolio balances data for particular timestamp.
type DataPoint struct {
	// UserID is the ID of the user where this portfolio balance belongs to.
	UserID string
	// Currency is the currency iso alpha codes for fiat and crypto currencies.
	Currency string
	// Amount is the portfolio balance in the specified timestamp.
	Amount *apd.Decimal
	// Timestamp is time of the balance record's timestamp.
	Timestamp time.Time
}

// DataPointPerGranularity holds data point per granularity.
type DataPointPerGranularity struct {
	Granularity Granulation
	Data        *DataPoint
}

// DataTimeRange hold time information for the earliest data timestamp and current timestamp.
type DataTimeRange struct {
	Earliest time.Time
	Current  time.Time
}

// Repository represents a storage to access portfolio history information.
//
//go:generate moq -out ./internal/mock/portfolio_repo.go -pkg mock -fmt goimports . Repository
type Repository interface {
	// InsertBulk inserts data-point in a bulk.
	// Raise ErrorCodeInternal in case of any errors.
	InsertBulk(ctx context.Context, data []*DataPointPerGranularity) error
	// UpsertDatapoint do upsert on some datapoints.
	// This is slow operation.
	UpsertDatapoint(ctx context.Context, data []*DataPointPerGranularity) error
}

// DataPointMessage holds data point from Kafka message.
type DataPointMessage struct {
	// UserID is the ID of the user where this portfolio balance belongs to.
	UserID string
	// Currency is the currency iso alpha codes for fiat and crypto currencies.
	Currency string
	// Timestamp is time of the balance record's timestamp.
	Timestamp time.Time
	// Amount is the portfolio balance in the specified timestamp.
	Amount *apd.Decimal
	// SentAt is time of when the kafka message was sent.
	SentAt time.Time
}

// KafkaMsg is an interface for kafka message.
//
//go:generate moq -out ./internal/mock/kafka_msg.go -pkg mock -fmt goimports . KafkaMsg
type KafkaMsg interface {
	// Msg returns message in slice of byte.
	Msg() []byte
	// Timestamp return message timestamp.
	Timestamp() time.Time
	// Partition return message partition id.
	Partition() int
	// Offset return message offset.
	Offset() int64
	// Commit commits message when it is done processing.
	Commit(ctx context.Context) error
}

// KafkaReader is an interface for kafka reader.
//
//go:generate moq -out ./internal/mock/kafka_reader.go -pkg mock -fmt goimports . KafkaReader
type KafkaReader interface {
	// FetchMessage fetches one message in blocking mode.
	FetchMessage(ctx context.Context) (KafkaMsg, error)
	// Close closes kafka reader.
	Close() error
}

// KafkaReaderFn function that returns KafkaReader for init
type KafkaReaderFn func() (KafkaReader, error)

// PendingKafkaService represents a service to process pending kafka messages.
//
//go:generate moq -out ./internal/mock/pending_kafka_svc.go -pkg mock -fmt goimports . PendingKafkaService
type PendingKafkaService interface {
	RunKafkaHandler(ctx context.Context, kafkaReader KafkaReader, lastOffset int64) error
}

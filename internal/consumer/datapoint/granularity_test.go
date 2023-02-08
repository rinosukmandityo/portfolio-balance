package datapoint_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	portfolio "github.com/rinosukmandityo/portfolio-balance"
	"github.com/rinosukmandityo/portfolio-balance/internal/consumer/datapoint"
)

func TestGranularity(t *testing.T) {
	testTable := []struct {
		name           string
		timestamp      time.Time
		expGranularity portfolio.Granulation
	}{
		{
			name:           "week",
			timestamp:      time.Date(2022, 9, 26, 0, 0, 0, 0, time.UTC),
			expGranularity: portfolio.WeekGranulation,
		},
		{
			name:           "day",
			timestamp:      time.Date(2022, 9, 22, 0, 0, 0, 0, time.UTC),
			expGranularity: portfolio.DayGranulation,
		},
		{
			name:           "12hours",
			timestamp:      time.Date(2022, 9, 22, 12, 0, 0, 0, time.UTC),
			expGranularity: portfolio.TwelveHoursGranulation,
		},
		{
			name:           "6hours",
			timestamp:      time.Date(2022, 9, 22, 6, 0, 0, 0, time.UTC),
			expGranularity: portfolio.SixHoursGranulation,
		},
		{
			name:           "1hour",
			timestamp:      time.Date(2022, 9, 22, 1, 0, 0, 0, time.UTC),
			expGranularity: portfolio.HourGranulation,
		},
		{
			name:           "15mins",
			timestamp:      time.Date(2022, 9, 22, 1, 15, 0, 0, time.UTC),
			expGranularity: portfolio.FifteenMinutesGranulation,
		},
		{
			name:           "invalid_minute_error",
			timestamp:      time.Date(2022, 9, 22, 1, 16, 0, 0, time.UTC),
			expGranularity: portfolio.Granulation(""),
		},
		{
			name:           "invalid_second_success",
			timestamp:      time.Date(2022, 9, 22, 1, 30, 25, 0, time.UTC),
			expGranularity: portfolio.FifteenMinutesGranulation,
		},
	}

	for _, tt := range testTable {
		t.Run(tt.name, func(t *testing.T) {
			res := datapoint.GetGranularity(tt.timestamp)
			require.Equal(t, tt.expGranularity, res)
		})
	}
}

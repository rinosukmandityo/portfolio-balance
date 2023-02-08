package datapoint

import (
	"time"

	"github.com/rinosukmandityo/portfolio-balance"
)

func GetGranularity(timestamp time.Time) portfolio.Granulation {
	var granularity portfolio.Granulation

	switch {
	case timestamp.Weekday() == time.Monday && timestamp.Hour() == 0 && timestamp.Minute() == 0:
		granularity = portfolio.WeekGranulation
	case timestamp.Hour() == 0 && timestamp.Minute() == 0:
		granularity = portfolio.DayGranulation
	case timestamp.Hour()%12 == 0 && timestamp.Minute() == 0:
		granularity = portfolio.TwelveHoursGranulation
	case timestamp.Hour()%6 == 0 && timestamp.Minute() == 0:
		granularity = portfolio.SixHoursGranulation
	case timestamp.Minute() == 0:
		granularity = portfolio.HourGranulation
	case timestamp.Minute()%15 == 0:
		granularity = portfolio.FifteenMinutesGranulation
	}

	return granularity
}

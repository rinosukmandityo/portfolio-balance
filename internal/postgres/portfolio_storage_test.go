package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/suite"

	"github.com/rinosukmandityo/portfolio-balance"
)

const (
	testUserID   = "1"
	testUserID2  = "2"
	testCurrency = "PHP"
)

type BasePortfolioStorageTestSuite struct {
	baseDBTestSuite
	storage *PortfolioStorage
}

func (bps *BasePortfolioStorageTestSuite) SetupTest() {
	bps.client.createSchema()
	bps.storage = NewPortfolioStorage(bps.client.client)
}

func (bps *BasePortfolioStorageTestSuite) TearDownTest() {
	bps.client.tearDown()
}

type PortfolioStorageTestSuite struct {
	BasePortfolioStorageTestSuite

	dataPoint *portfolio.DataPoint
}

func (ps *PortfolioStorageTestSuite) SetupTest() {
	ps.BasePortfolioStorageTestSuite.SetupTest()
	ps.dataPoint = makeDataPoint()
}

func (ps *PortfolioStorageTestSuite) TestInsert_Success() {
	ps.Require().NoError(ps.storage.InsertBulk(context.Background(),
		[]*portfolio.DataPointPerGranularity{
			{
				Granularity: portfolio.FifteenMinutesGranulation,
				Data:        ps.dataPoint,
			},
		},
	))

	var models []*dataPointModel
	ps.Require().NoError(ps.client.conn.Select(&models, "SELECT * FROM portfolio_balance"))
	ps.Require().Len(models, 1)
	ps.Require().Empty(cmp.Diff(
		models[0],
		makeModelFromDataPoint(ps.dataPoint, portfolio.FifteenMinutesGranulation),
		cmpopts.EquateApproxTime(time.Minute),
		cmpopts.IgnoreFields(dataPointModel{}, "CreatedAt"),
		decimalComparer(),
	))
}

func (ps *PortfolioStorageTestSuite) TestInsert_AvailableGranulation_Success() {
	granulations := []portfolio.Granulation{
		portfolio.FifteenMinutesGranulation,
		portfolio.HourGranulation,
		portfolio.SixHoursGranulation,
		portfolio.TwelveHoursGranulation,
		portfolio.DayGranulation,
		portfolio.WeekGranulation,
	}

	for _, granulation := range granulations {
		ps.dataPoint.Timestamp = time.Now().Add(time.Second)
		ps.Require().NoError(
			ps.storage.InsertBulk(context.Background(),
				[]*portfolio.DataPointPerGranularity{
					{
						Granularity: granulation,
						Data:        ps.dataPoint,
					},
				},
			),
			"granulation is", granulation,
		)
	}

	var models []*dataPointModel
	ps.Require().NoError(ps.client.conn.Select(&models, "SELECT * FROM portfolio_balance"))
	ps.Require().Len(models, len(granulations))
}

func (ps *PortfolioStorageTestSuite) TestInsert_MassiveDuplication_Success() {
	bucketSize := 100_000
	data := make([]*portfolio.DataPointPerGranularity, 0, bucketSize)
	tNow := time.Now().UTC()
	for i := 0; i < bucketSize; i++ {
		tNow = tNow.Add(time.Second)
		data = append(data, &portfolio.DataPointPerGranularity{
			Granularity: portfolio.HourGranulation,
			Data: &portfolio.DataPoint{
				UserID:    "123",
				Currency:  "PHP",
				Amount:    apd.New(10, 0),
				Timestamp: tNow,
			},
		})
	}
	ps.Require().NoError(ps.storage.InsertBulk(context.Background(), data))
	var models []*dataPointModel
	ps.Require().NoError(ps.client.conn.Select(&models, "SELECT * FROM portfolio_balance"))
	ps.Require().Len(models, bucketSize)
	// duplicate only 1 data
	ps.Require().Error(ps.storage.InsertBulk(context.Background(), data[:1]))
	models = models[:0]
	ps.Require().NoError(ps.client.conn.Select(&models, "SELECT * FROM portfolio_balance"))
	ps.Require().Len(models, bucketSize)
	// duplicate some data
	ps.Require().Error(ps.storage.InsertBulk(context.Background(), data[100:1_000]))
	models = models[:0]
	ps.Require().NoError(ps.client.conn.Select(&models, "SELECT * FROM portfolio_balance"))
	ps.Require().Len(models, bucketSize)
	// duplicate all data
	ps.Require().Error(ps.storage.InsertBulk(context.Background(), data))
	models = models[:0]
	ps.Require().NoError(ps.client.conn.Select(&models, "SELECT * FROM portfolio_balance"))
	ps.Require().Len(models, bucketSize)
}

func (ps *PortfolioStorageTestSuite) TestInsert_Upsert_Success() {
	ps.Require().NoError(ps.storage.InsertBulk(context.Background(),
		[]*portfolio.DataPointPerGranularity{
			{
				Granularity: portfolio.FifteenMinutesGranulation,
				Data:        ps.dataPoint,
			},
		},
	))

	var oldmodels []*dataPointModel
	ps.Require().NoError(ps.client.conn.Select(&oldmodels, "SELECT * FROM portfolio_balance ORDER BY issued_at"))
	ps.Require().Len(oldmodels, 1)

	time.Sleep(1 * time.Second)

	data1 := *ps.dataPoint
	data1.Amount, _, _ = apd.NewFromString("999.99123")
	data2 := *ps.dataPoint
	data2.Timestamp = data2.Timestamp.Add(30 * time.Minute)
	data3 := *ps.dataPoint
	data3.Timestamp = data2.Timestamp.Add(45 * time.Minute)

	ps.Require().NoError(ps.storage.upsertSlow(context.Background(),
		[]*portfolio.DataPointPerGranularity{
			{
				Granularity: portfolio.FifteenMinutesGranulation,
				Data:        &data1,
			},
			{
				Granularity: portfolio.FifteenMinutesGranulation,
				Data:        &data2,
			},
			{
				Granularity: portfolio.FifteenMinutesGranulation,
				Data:        &data3,
			},
		},
	))

	var models []*dataPointModel
	ps.Require().NoError(ps.client.conn.Select(&models, "SELECT * FROM portfolio_balance ORDER BY issued_at"))
	ps.Require().Len(models, 3)
	models[0].Amount, _ = models[0].Amount.Reduce(models[0].Amount)
	ps.Require().Equal("999.99123", models[0].Amount.String())
	ps.Require().Greater(models[0].CreatedAt.Unix(), oldmodels[0].CreatedAt.Unix())
	ps.Require().Equal(models[0].IssuedAt.Unix(), oldmodels[0].IssuedAt.Unix())
}

func TestPortfolioStorage(t *testing.T) {
	suite.Run(t, new(PortfolioStorageTestSuite))
}

func makeDataPoint() *portfolio.DataPoint {
	return &portfolio.DataPoint{
		UserID:    testUserID,
		Currency:  testCurrency,
		Amount:    apd.New(10, -18),
		Timestamp: time.Now(),
	}
}

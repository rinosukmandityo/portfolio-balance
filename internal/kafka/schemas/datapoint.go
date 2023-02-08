package schemas

import (
	"fmt"
	"time"

	"github.com/cockroachdb/apd"

	"github.com/rinosukmandityo/portfolio-balance"
)

// DataPointSchema is a schema for data points message.
const DataPointSchema = `{
  "namespace": "company.portfoliobalance",
  "type": "record",
  "name": "HistoricalPortfolioBalance",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "currency", "type": "string"},
    {"name": "period", "type": {"type": "long","logicalType": "timestamp-micros"}},
    {"name": "amount", "type": "string"},
    {"name": "sent_at", "type": {"type": "long","logicalType": "timestamp-micros"}}
  ]
}`

func DecodeDataPoint(decoded map[string]interface{}) (*portfolio.DataPointMessage, error) {
	var ok bool
	var err error
	tx := &portfolio.DataPointMessage{}
	tx.UserID, ok = decoded["user_id"].(string)
	if !ok {
		return nil, fmt.Errorf("cannot cast 'user_id' field to string from kafka message, type: %T, value: %v",
			decoded["user_id"], decoded["user_id"])
	}
	tx.Currency, ok = decoded["currency"].(string)
	if !ok {
		return nil, fmt.Errorf("cannot cast 'currency' field to string from kafka message, type: %T, value: %v",
			decoded["currency"], decoded["currency"])
	}

	tx.Timestamp, ok = decoded["period"].(time.Time)
	if !ok {
		return nil, fmt.Errorf("cannot cast 'period' field to time.Time from kafka message, type: %T, value: %v",
			decoded["period"], decoded["period"])
	}
	amount, ok := decoded["amount"].(string)
	if !ok {
		return nil, fmt.Errorf("cannot cast 'amount' field to bytes from kafka message, type: %T, value: %v",
			decoded["amount"], decoded["amount"])
	}
	tx.Amount, _, err = apd.NewFromString(amount)
	if err != nil {
		return nil, fmt.Errorf("cannot create a new decimal from amount")
	}
	tx.SentAt, ok = decoded["sent_at"].(time.Time)
	if !ok {
		return nil, fmt.Errorf("cannot cast 'sent_at' field to time.Time from kafka message, type: %T, value: %v",
			decoded["sent_at"], decoded["sent_at"])
	}

	return tx, nil
}

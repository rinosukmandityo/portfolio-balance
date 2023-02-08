package postgres

// Portfolio Storage Queries.
const (
	selectUserTimeRange = `SELECT MIN(issued_at) AS earliest, NOW() AS current FROM portfolio_balance WHERE user_id = $1 AND currency = $2`

	upsertPortfolioBalance = `
INSERT INTO portfolio_balance (user_id, currency, amount, granularity, issued_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT ON CONSTRAINT portfolio_balance_pkey DO UPDATE SET amount=EXCLUDED.amount, created_at=NOW()`
)

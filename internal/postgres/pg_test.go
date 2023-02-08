package postgres

import (
	"os"

	"github.com/cockroachdb/apd"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/suite"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

// client is a test wrapper for pg repositories.
type client struct {
	// conn is a connection to a test db to create schema.
	conn *sqlx.DB

	// sysConn is a connection to "postgres" db to drop/create a test db.
	sysConn *sqlx.DB
	client  *Client
}

func mustOpenClient() *client {
	logger, _ := zap.NewDevelopment()
	cl := NewClient(logger)
	tc := &client{
		client: cl,
	}

	tc.open()
	return tc
}

// open obtain all required connections and opens pg.Client.
// All existing databases are dropped and created from scratch with required schema.
// Configuration should be done via env variables (as for libpq):
// - PGHOST
// - PGPORT
// - PGDATABASE
// - PGUSER
// - PGPASSWORD
// Other required env variables can be passed for additional configuration,
// see https://www.postgresql.org/docs/current/libpq-envars.html for reference.
func (c *client) open() {
	dbName := os.Getenv("PGDATABASE")

	c.sysConn = sqlx.MustConnect("postgres", "dbname=postgres binary_parameters=yes")
	c.sysConn.MustExec("DROP DATABASE IF EXISTS " + dbName)
	c.sysConn.MustExec("CREATE DATABASE " + dbName)

	c.conn = sqlx.MustConnect("postgres", "")
	_ = c.client.Open("")
}

func (c *client) createSchema() {
	c.conn.MustExec(scheme)
}

func (c *client) tearDown() {
	c.conn.MustExec(schemeDown)
}

func (c *client) close() {
	_ = c.conn.Close()
	_ = c.client.Close()
	c.sysConn.MustExec("DROP DATABASE IF EXISTS " + os.Getenv("PGDATABASE"))
	_ = c.sysConn.Close()
}

type baseDBTestSuite struct {
	suite.Suite
	client *client
}

func (suite *baseDBTestSuite) SetupSuite() {
	suite.client = mustOpenClient()
}

func (suite *baseDBTestSuite) TearDownSuite() {
	if suite.client != nil {
		suite.client.close()
	}
}

func decimalComparer() cmp.Option {
	return cmp.Comparer(func(d1 *apd.Decimal, d2 *apd.Decimal) bool {
		if d1 == d2 {
			return true
		}

		if d1 == nil || d2 == nil {
			return false
		}

		return d1.Cmp(d2) == 0
	})
}

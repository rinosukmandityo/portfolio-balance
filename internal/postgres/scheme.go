package postgres

const scheme = `
CREATE TYPE granulation AS ENUM (
    '15min',
    '1hour',
    '6hours',
    '12hours',
    '1day',
	'1week'
);

CREATE TABLE IF NOT EXISTS portfolio_balance (
    user_id     VARCHAR(64) NOT NULL,
    currency    VARCHAR(3) NOT NULL,
    amount      NUMERIC(29, 18) NOT NULL,
    granularity granulation NOT NULL,
    issued_at   TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY(user_id, currency, issued_at)
);

CREATE TABLE IF NOT EXISTS pending_kafka_props (
    id varchar(26) NOT NULL PRIMARY KEY,
    start_offset bigint NOT NULL,
    last_offset bigint NOT NULL,
    partition int NOT NULL,
    is_processed boolean NOT NULL
);

ALTER TABLE IF EXISTS "pending_kafka_props" ADD CONSTRAINT "pending_kafka_props_uniq" UNIQUE (start_offset, last_offset, partition);

CREATE INDEX is_processed_idx ON pending_kafka_props USING btree (id) where is_processed = false;
`

const schemeDown = `
DROP INDEX IF EXISTS is_processed_idx;
ALTER TABLE IF EXISTS pending_kafka_props DROP CONSTRAINT pending_kafka_props_uniq;
DROP TABLE IF EXISTS portfolio_balance;
DROP TABLE IF EXISTS pending_kafka_props;
DROP FUNCTION IF EXISTS remove_old_data;
DROP TYPE IF EXISTS granulation;
`

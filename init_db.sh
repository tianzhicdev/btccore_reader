#!/bin/bash

# Check if environment parameter is provided
if [ "$1" != "local" ] && [ "$1" != "server" ]; then
    echo "Usage: $0 [local|server]"
    exit 1
fi

# Set postgres data directory based on environment
if [ "$1" == "local" ]; then
    PGDATA=~/postgres_data
else
    PGDATA=/Volumes/4tb0/postgres_data

fi

# Create postgres data directory if it doesn't exist
mkdir -p $PGDATA

# Check if postgres is already initialized
if [ ! -f "$PGDATA/PG_VERSION" ]; then
    # Initialize postgres database cluster
    initdb -D $PGDATA
fi

# Check if postgres is running
pg_ctl status -D $PGDATA > /dev/null
if [ $? -ne 0 ]; then
    # Start postgres server with custom data directory
    pg_ctl -D $PGDATA -l $PGDATA/logfile start
    
    # Wait for postgres to start
    sleep 3
fi

# Create user and database if they don't exist
psql -p 3004 postgres << EOF
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'abc') THEN
        CREATE USER abc WITH PASSWORD '12345';
    END IF;
END
\$\$;

SELECT 'CREATE DATABASE bitcoin' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'bitcoin')\gexec
GRANT ALL PRIVILEGES ON DATABASE bitcoin TO abc;
EOF

# Create table and indexes if they don't exist
psql -p 3004 -U abc bitcoin -e << EOF
\set QUIET off
\echo 'Creating blocks table if it does not exist...'
CREATE TABLE IF NOT EXISTS blocks (
    block_number INTEGER UNIQUE,
    blockhash VARCHAR(64) UNIQUE,
    data JSON
);

\echo 'Creating indexes on blocks table if they do not exist...'
CREATE INDEX IF NOT EXISTS idx_block_number ON blocks(block_number);
CREATE INDEX IF NOT EXISTS idx_blockhash ON blocks(blockhash);

\echo 'Creating transactions table if it does not exist...'
CREATE TABLE transactions (
    tx_id VARCHAR(64),
    block_number INTEGER,
    data JSON,
    CONSTRAINT transactions_unique_tx UNIQUE (tx_id, block_number)
);

\echo 'Creating balances table if it does not exist...'
CREATE TABLE IF NOT EXISTS balances (
    timestamp TIMESTAMP,
    address VARCHAR(255),
    amount NUMERIC,
    txid VARCHAR(64),
    block_number INTEGER,
    UNIQUE (timestamp, address, amount, txid, block_number)
);

\echo 'Creating indexes on balances table if they do not exist...'
CREATE INDEX IF NOT EXISTS idx_balances_timestamp ON balances(timestamp);
CREATE INDEX IF NOT EXISTS idx_balances_address ON balances(address);
CREATE INDEX IF NOT EXISTS idx_balances_txid ON balances(txid);
CREATE INDEX IF NOT EXISTS idx_balances_block_number ON balances(block_number);

\echo 'Creating unprocessed_transactions table if it does not exist...'
CREATE TABLE IF NOT EXISTS unprocessed_transactions (
    tx_id VARCHAR(64) PRIMARY KEY
);


\echo '\n'
\echo 'Done.'
EOF

echo "Database initialized successfully!"

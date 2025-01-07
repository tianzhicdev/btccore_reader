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
psql postgres << EOF
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
psql -U abc bitcoin -e << EOF
\set QUIET off
\echo 'Creating transactions table if it does not exist...'
CREATE TABLE IF NOT EXISTS transactions (
    timestamp DATE,
    address VARCHAR(100),
    amount DOUBLE PRECISION,
    tx VARCHAR(100),
    UNIQUE(timestamp, address, amount, tx)
);
\echo '\n'

\echo 'Inserting test record...'
INSERT INTO transactions (timestamp, address, amount, tx) 
VALUES ('2023-01-01', '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa', 50.0, 'test_tx_123')
ON CONFLICT (timestamp, address, amount, tx) DO NOTHING;
\echo '\n'

\echo 'Fetching test record:'
\x on
SELECT * FROM transactions WHERE tx = 'test_tx_123';
\x off
\echo '\n'

\echo 'Deleting test record...'
DELETE FROM transactions WHERE tx = 'test_tx_123';
\echo '\n'

\echo 'Creating indexes if they do not exist...'
CREATE INDEX IF NOT EXISTS idx_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_address ON transactions(address);
\echo '\n'
\echo 'Done.'
EOF

echo "Database initialized successfully!"

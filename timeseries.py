import psycopg2
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import sys

# Set up logging
log_file = '/tmp/bitcoin_hodls.log'
logger = logging.getLogger('hodls')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(log_file, maxBytes=1024*1024*1024, backupCount=1)  # 1GB max
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

DURATION = 365
BALANCE = 1

# Establish a database connection
try:
    conn = psycopg2.connect(
        dbname="bitcoin",
        user="abc",
        password="12345",
        host="localhost",
        port="3004"
    )
    cursor = conn.cursor()
except psycopg2.Error as e:
    logger.error(f"Error connecting to the database: {e}")
    sys.exit(1)

def latest_transaction_date(transactions_table_name):
    try:
        query = f"SELECT MAX(timestamp) FROM {transactions_table_name};"
        cursor.execute(query)
        latest_transaction_date = cursor.fetchone()[0]
        if latest_transaction_date is None:
            return None
        latest_transaction_date = datetime(
            year=latest_transaction_date.year, 
            month=latest_transaction_date.month,
            day=latest_transaction_date.day,
        )
        logger.info(f"Max date from {transactions_table_name}: {latest_transaction_date.strftime('%Y-%m-%d')}")
        return latest_transaction_date
    except Exception as e:
        logger.error(f"Error fetching latest transaction date from {transactions_table_name}: {e}")
        sys.exit(1)

def latest_timeseries_date(hodls_table_name):
    try:
        query = f"SELECT MAX(date) FROM {hodls_table_name};"
        cursor.execute(query)
        latest_date = cursor.fetchone()[0]
        if latest_date is None:
            logger.info(f"No previous timeseries data found in table {hodls_table_name}, starting fresh.")
            return None
        latest_date = datetime(
            year=latest_date.year, 
            month=latest_date.month,
            day=latest_date.day,
        )
        logger.info(f"Max date from {hodls_table_name}: {latest_date.strftime('%Y-%m-%d')}")
        return latest_date
    
    except Exception as e:
        logger.error(f"Error fetching latest timeseries date for {hodls_table_name}: {e}")
        sys.exit(1)


def create_hodls_table(hodls_table_name):
    try:
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {hodls_table_name} (
            date DATE PRIMARY KEY,
            hodls INTEGER
        );
        """)
        conn.commit()
        logger.info(f"Table {hodls_table_name} checked/created successfully.")
    except Exception as e:
        logger.error(f"Error creating table {hodls_table_name}: {e}")
        sys.exit(1)


# Check if table names are provided
if len(sys.argv) > 2:
    transactions_table_name = sys.argv[1]
    hodls_table_name = sys.argv[2]
else:
    logger.error("Both transactions_table_name and hodls_table_name must be provided as parameters.")
    sys.exit(1)


create_hodls_table(hodls_table_name)

logger.info(f"Running script with transactions_table_name: {transactions_table_name} and hodls_table_name: {hodls_table_name}")

current_latest_transaction_date = latest_transaction_date(transactions_table_name)
latest_hodl_date = latest_timeseries_date(hodls_table_name)

if not current_latest_transaction_date:
    logger.info("No transactions found, exiting")
    sys.exit(0)
    

if latest_hodl_date:
    period_end_date = min(latest_hodl_date, current_latest_transaction_date)
else:
    period_end_date = datetime(2010, 1, 8)

period_end_date = period_end_date + timedelta(days=7)

logger.info(f"7-day period ending on: {period_end_date.strftime('%Y-%m-%d')}")

# Iterate over each week from the start date to the current date
while period_end_date <= datetime.now():
    try:
        if period_end_date < (current_latest_transaction_date - timedelta(days=1)):

            logger.info(f"Processing data for the week ending on: {period_end_date.strftime('%Y-%m-%d')}")
            query = f"""
            WITH hodler_count AS (
                SELECT COUNT(DISTINCT address) AS count FROM (
                    SELECT address
                    FROM {transactions_table_name} 
                    WHERE timestamp < %s
                    GROUP BY address
                    HAVING SUM(amount) > %s AND MIN(timestamp) < %s
                ) AS HODLER_ADDRESS
            )
            INSERT INTO {hodls_table_name} (date, hodls)
            VALUES (%s, (SELECT count FROM hodler_count))
            ON CONFLICT (date) DO UPDATE SET hodls = EXCLUDED.hodls;
            """
            cursor.execute(query, (period_end_date, BALANCE, period_end_date - timedelta(days=DURATION), period_end_date))
            logger.info(f"Inserted hodler count for date {period_end_date.strftime('%Y-%m-%d')} into table {hodls_table_name}. Number of records: 1")
            conn.commit()
        else:
            logger.info(f"Data is not yet available for {period_end_date.strftime('%Y-%m-%d')}.")
            break
    except Exception as e:
        logger.error(f"Error during iteration for date {period_end_date.strftime('%Y-%m-%d')}: {e}")
        conn.rollback()

    period_end_date += timedelta(weeks=1)

# Close the database connection
cursor.close()
conn.close()

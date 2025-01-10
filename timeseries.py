import psycopg2
from datetime import datetime, timedelta

import logging
from logging.handlers import RotatingFileHandler
# Set up logging
log_file = '/tmp/bitcoin_hodls.log'
logger = logging.getLogger('hodls')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(log_file, maxBytes=1024*1024*1024, backupCount=1) # 1GB max
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

DURATION = 365
BALANCE = 1

# Establish a database connection
conn = psycopg2.connect(
    dbname="bitcoin",
    user="abc",
    password="12345",
    host="localhost"
)
cursor = conn.cursor()

# Get the maximum date from the transactions table
cursor.execute("SELECT MAX(timestamp) FROM transactions;")
latest_transaction_date = cursor.fetchone()[0]
latest_transaction_date = datetime(
    year=latest_transaction_date.year, 
    month=latest_transaction_date.month,
    day=latest_transaction_date.day,
)
logger.info(f"Max date from transactions: {latest_transaction_date.strftime('%Y-%m-%d')}")

# Define the start date for iteration
cursor.execute("SELECT MAX(date) FROM hodls;")
latest_timeseries_date = cursor.fetchone()[0]
logger.info(f"Current max date from hodls: {latest_timeseries_date}")
if latest_timeseries_date:
    start_date = min(datetime(year=latest_timeseries_date.year, month=latest_timeseries_date.month, day=latest_timeseries_date.day), latest_transaction_date)
else:
    start_date = datetime(2010, 1, 8)
logger.info(f"Start date for iteration: {start_date.strftime('%Y-%m-%d')}")

# Iterate over each week from the start date to the current date
current_date = start_date
while current_date <= datetime.now():
    if current_date < (latest_transaction_date - timedelta(days=1)):
        # Data is up to date for the current week
        query = """
        SELECT COUNT(DISTINCT address) FROM (
            SELECT address
            FROM transactions 
            WHERE timestamp < %s
            GROUP BY address
            HAVING SUM(amount) > %s AND MIN(timestamp) < %s
        ) AS HODLER_ADDRESS;
        """
        cursor.execute(query, (current_date, BALANCE, current_date - timedelta(days=DURATION)))
        hodler_count = cursor.fetchone()[0]
        logger.info(f"Number of hodlers for week starting {current_date.strftime('%Y-%m-%d')}: {hodler_count}")
    # Insert the hodler count into the hodls table
        insert_query = """
        INSERT INTO hodls (date, hodls) VALUES (%s, %s)
        ON CONFLICT (date) DO UPDATE SET hodls = EXCLUDED.hodls;
        """
        cursor.execute(insert_query, (current_date, hodler_count))
        conn.commit()
    # Fetch and print some addresses from the HODLER_ADDRESS subquery

    # Move to the next week
    current_date += timedelta(weeks=1)

# Close the database connection
cursor.close()
conn.close()

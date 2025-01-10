import psycopg2
from datetime import datetime, timedelta

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
MAX_DATE = cursor.fetchone()[0]
MAX_DATE = datetime(
    year=MAX_DATE.year, 
    month=MAX_DATE.month,
    day=MAX_DATE.day,
)


# Define the start date for iteration
start_date = datetime(2010, 1, 8)

# Iterate over each week from the start date to the current date
current_date = start_date
while current_date <= datetime.now():
    if current_date < (MAX_DATE - timedelta(days=1)):
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
        print(f"Number of hodlers for week starting {current_date.strftime('%Y-%m-%d')}: {hodler_count}")
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

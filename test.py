import psycopg2
import sys
import logging

# Set up logging
logger = logging.getLogger('test_db_connection')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

try:
    conn = psycopg2.connect(
        dbname="bitcoin",
        user="abc",
        password="12345",
        host="192.168.0.49",
        port="3004"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()
    logger.info(f"Test query result: {result}")
except psycopg2.Error as e:
    logger.error(f"Error connecting to the database: {e}")
    sys.exit(1)
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
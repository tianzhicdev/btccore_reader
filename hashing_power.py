from utils import get_rpc_connection_user_pw
from btccore_reader.blocks import *

rpc_connection = get_rpc_connection_user_pw()
db_conn = create_db_connection()
db_cursor = db_conn.cursor()

# Create the table if it does not exist
create_table_query = """
CREATE TABLE IF NOT EXISTS difficulty (
    timestamp TIMESTAMP,
    block_number INTEGER,
    value DOUBLE PRECISION,
    PRIMARY KEY (timestamp)
);
"""

# Execute the table creation query
db_cursor.execute(create_table_query)
db_conn.commit()

# Loop through the blocks in the Bitcoin Core
def process_blocks():
    try:
        block_count = rpc_connection.getblockcount()
        for block_number in range(block_count):
            block_hash = rpc_connection.getblockhash(block_number)
            block = rpc_connection.getblock(block_hash)
            
            # Extract timestamp and hashing power
            timestamp = datetime.fromtimestamp(block['time'])
            hashing_power = block['difficulty']  # Assuming difficulty represents hashing power
            
            # Insert into the table
            insert_query = """
            INSERT INTO difficulty (timestamp, value)
            VALUES (%s, %s)
            ON CONFLICT (timestamp) DO NOTHING;
            """
            db_cursor.execute(insert_query, (timestamp, hashing_power))

            db_conn.commit()
            
    except Exception as e:
        logger.error(f"Error processing blocks: {e}")

# Call the function to process blocks
process_blocks()

# if __name__ == '__main__':
#     try:
#         db_cursor.execute("SELECT COALESCE(MAX(block_number), 0) FROM transactions")
#         max_block_num = db_cursor.fetchone()[0]
#         logger.info(f"Starting from block number: {max_block_num}")
#         block_num = max(1, max_block_num - 1)
#         while True:
#             if not process_block(block_num):
#                 break
#             block_num += 1

    # except JSONRPCException as e:
    #     logger.error(f"Fatal RPC error: {e}")

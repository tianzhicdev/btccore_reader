import sys
from utils import get_logger, get_rpc_connection_user_pw, create_db_connection
from datetime import datetime
from bitcoinrpc.authproxy import JSONRPCException

logger = get_logger("difficulty")

rpc_connection = get_rpc_connection_user_pw()
db_conn = create_db_connection()
db_cursor = db_conn.cursor()

# Create the table if it does not exist
def create_difficulty_table(difficulty_table_name):
    try:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {difficulty_table_name} (
            timestamp TIMESTAMP,
            block_number DOUBLE PRECISION,
            value DOUBLE PRECISION,
            PRIMARY KEY (block_number)
        );
        """
        # Execute the table creation query
        db_cursor.execute(create_table_query)
        db_conn.commit()
        logger.info(f"Table {difficulty_table_name} checked/created successfully.")
    except Exception as e:
        logger.error(f"Error creating table {difficulty_table_name}: {e}")
        sys.exit(1)

# Loop through the blocks in the Bitcoin Core
def process_blocks(block_number):
    try:
        block_hash = rpc_connection.getblockhash(block_number)
        block = rpc_connection.getblock(block_hash)
        
        # Extract timestamp and hashing power
        timestamp = datetime.fromtimestamp(block['time'])
        difficulty = block['difficulty']  # Assuming difficulty represents hashing power
        
        # Insert into the table
        insert_query = """
        INSERT INTO difficulty (timestamp, block_number, value)
        VALUES (%s, %s, %s)
        ON CONFLICT (block_number) DO NOTHING;
        """
        db_cursor.execute(insert_query, (timestamp, block_number, difficulty))
        logger.info(f"Inserted difficulty record for timestamp {timestamp} with block number {block_number} and value {difficulty}")

        db_conn.commit()
            
    except Exception as e:
        logger.error(f"Error processing blocks: {e}")

if __name__ == '__main__':
    # Check if table names are provided
    if len(sys.argv) > 1:
        difficulty_table_name = sys.argv[1]
    else:
        logger.error("difficulty_table_name must be provided as a parameter.")
        sys.exit(1)
    
    create_difficulty_table(difficulty_table_name)

    try:
        # Get the latest block_number from the difficulty table
        db_cursor.execute(f"SELECT COALESCE(MAX(block_number), 1) FROM {difficulty_table_name}")
        max_block_num = db_cursor.fetchone()[0]
        logger.info(f"Starting from block number: {max_block_num}")
        
        # Start from the block before the latest one
        block_num = int(max(1, max_block_num - 1))
        
        while True:
            try:
                block_hash = rpc_connection.getblockhash(block_num)
                if not block_hash:
                    logger.info(f"Block number {block_num} is not available in Bitcoin Core. Exiting.")
                    break
                
                # Process the block
                process_blocks(block_num)
                block_num += 1

            except JSONRPCException as e:
                logger.error(f"RPC error while processing block number {block_num}: {e}")
                break

    except Exception as e:
        logger.error(f"Error initializing block processing: {e}")

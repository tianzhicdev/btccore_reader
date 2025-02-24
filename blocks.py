from bitcoinrpc.authproxy import JSONRPCException
import json
from utils import get_logger
from utils import get_rpc_connection_user_pw, create_db_connection


logger = get_logger("blocks")

rpc_connection = get_rpc_connection_user_pw()

db_conn = create_db_connection()
db_cursor = db_conn.cursor()

def process_block(block_num):
    retries = 5
    while retries > 0:
        try:
            block_hash = rpc_connection.getblockhash(block_num)
            block = rpc_connection.getblock(block_hash, 2)
            db_cursor.execute(
                """
                INSERT INTO blocks (block_number, blockhash, data)
                VALUES (%s, %s, %s)
                ON CONFLICT (block_number) DO NOTHING
                """,
                (block_num, block_hash, json.dumps(block, default=str))
            )
            db_conn.commit()
            logger.info(f"Successfully processed block {block_num} with hash {block_hash}")
            return True
        except Exception as e:
            logger.error(f"Error processing block {block_num}: {e}")
            retries -= 1
            if retries == 0:
                return False

if __name__ == '__main__':
    try:
        db_cursor.execute("SELECT COALESCE(MAX(block_number), 0) FROM blocks")
        max_block_num = db_cursor.fetchone()[0]
        block_num = max(1, max_block_num - 1)
        logger.info(f"Starting from block number: {block_num}")
        while True:
            if not process_block(block_num):
                break
            block_num += 1

    except JSONRPCException as e:
        logger.error(f"Fatal RPC error: {e}")

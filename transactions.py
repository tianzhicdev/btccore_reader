from bitcoinrpc.authproxy import JSONRPCException
from utils import get_logger
from utils import get_rpc_connection_user_pw, create_db_connection
from transactions_utils import process_block_transactions_rpc


logger = get_logger("transactions")

rpc_connection = get_rpc_connection_user_pw()
db_conn = create_db_connection()
db_cursor = db_conn.cursor()

def single_main():
    try:
        db_cursor.execute("SELECT COALESCE(MAX(block_number), 0) FROM transactions")
        max_block_num = db_cursor.fetchone()[0]
        block_num = max(1, max_block_num - 1)
        logger.info(f"Starting from block number: {block_num}")
        while True:
            success = process_block_transactions_rpc(block_num, db_conn, rpc_connection, logger)
            if not success:
                break
            block_num += 1

    except JSONRPCException as e:
        logger.error(f"Fatal RPC error: {e}")

from concurrent.futures import ThreadPoolExecutor, as_completed

def process_blocks_in_parallel(start_block_num, num_blocks=10):
    try:
        with ThreadPoolExecutor(max_workers=num_blocks) as executor:
            future_to_block = {
                executor.submit(process_block_transactions, block_num, db_conn, rpc_connection, logger): block_num
                for block_num in range(start_block_num, start_block_num + num_blocks)
            }
            for future in as_completed(future_to_block):
                block_num = future_to_block[future]
                try:
                    success = future.result()
                    if success:
                        logger.info(f"Successfully processed block number: {block_num}")
                    else:
                        logger.error(f"Failed to process block number: {block_num}")
                except Exception as e:
                    logger.error(f"Exception occurred while processing block number {block_num}: {e}")
    except Exception as e:
        logger.error(f"Error in processing blocks in parallel: {e}")


if __name__ == '__main__':
    single_main()

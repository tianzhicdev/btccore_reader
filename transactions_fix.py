from bitcoinrpc.authproxy import JSONRPCException
from utils import get_logger
from utils import get_rpc_connection_user_pw, create_db_connection
from transactions_utils import process_block_transactions
import sys


logger = get_logger("transactions_fix")

rpc_connection = get_rpc_connection_user_pw()
db_conn = create_db_connection()
db_cursor = db_conn.cursor()

if __name__ == '__main__':
    try:
        # Query for distinct block numbers where transaction data is null
        db_cursor.execute("SELECT DISTINCT block_number FROM transactions WHERE data IS NULL")
        blocks_to_reprocess = db_cursor.fetchall()

        if not blocks_to_reprocess:
            logger.info("No blocks with null transaction data found.")
        else:
            logger.info(f"Found {len(blocks_to_reprocess)} blocks with null transaction data. Reprocessing...")

            for (block_num,) in blocks_to_reprocess:
                logger.info(f"Reprocessing block number: {block_num}")
                success = process_block_transactions(block_num, db_conn, logger)
                if not success:
                    logger.error(f"Failed to reprocess block number: {block_num}")
                    break

    except Exception as e:
        logger.error(f"Error during reprocessing of blocks: {e}")

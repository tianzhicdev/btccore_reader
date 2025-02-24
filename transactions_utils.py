from bitcoinrpc.authproxy import JSONRPCException
import json
from utils import get_logger
from utils import get_rpc_connection_user_pw, create_db_connection


logger = get_logger("transactions_utils")

rpc_connection = get_rpc_connection_user_pw()
db_conn = create_db_connection()
db_cursor = db_conn.cursor()


def get_transaction(transaction_id, block_num, block_hash, rpc_connection, logger, retries=5):
    while retries > 0:
        try:
            raw_transaction = rpc_connection.getrawtransaction(transaction_id, True, block_hash)
            logger.info(f"Retrieved raw transaction for {transaction_id} of block {block_num} with size {len(json.dumps(raw_transaction, default=str))} bytes")
            return raw_transaction
        except Exception as e:
            logger.error(f"Error retrieving raw transaction for {transaction_id}: {e}")
            retries -= 1
            if retries == 0:
                raise Exception(f"Failed to retrieve raw transaction for {transaction_id} after 5 retries")

def process_block_transactions_rpc(block_num, db_conn, rpc_connection, logger):
    retries = 5
    while retries > 0:
        try:
            block_hash = rpc_connection.getblockhash(block_num)
            block = rpc_connection.getblock(block_hash, 2)
            transactions = block.get('tx', [])
            logger.info(f"Fetched {len(transactions)} transactions in block {block_num}.")
            transaction_data = []
            for tx in transactions:
                transaction_data.append((tx["txid"], block_num, json.dumps(tx, default=str)))
            
            db_cursor = db_conn.cursor()
            db_cursor.executemany(
                """
                INSERT INTO transactions (tx_id, block_number, data)
                VALUES (%s, %s, %s)
                ON CONFLICT (tx_id, block_number) DO UPDATE SET data = EXCLUDED.data
                """,
                transaction_data
            )
            db_conn.commit()
            logger.info(f"Successfully inserted {len(transaction_data)} transactions for block {block_num}")
            return True
        except (Exception, JSONRPCException) as e:
            logger.error(f"Error processing block {block_num}: {e}")
            retries -= 1
            if retries == 0:
                return False

def process_block_transactions(block_num, db_conn, rpc_connection, logger):
    db_cursor = db_conn.cursor()
    retries = 5
    while retries > 0:
        try:
            db_cursor.execute("SELECT blockhash, data FROM blocks WHERE block_number = %s", (block_num,))
            result = db_cursor.fetchone()
            if result:
                block_hash, block_data = result
            else:
                raise ValueError(f"Block number {block_num} not found in database")
            
            transactions = block_data.get('tx', [])
            transaction_data = []
            for transaction_id in transactions:
                raw_transaction = get_transaction(transaction_id, block_num, block_hash, rpc_connection, logger)
                transaction_data.append((transaction_id, block_num, json.dumps(raw_transaction, default=str)))
            
            if transaction_data:
                for i in range(0, len(transaction_data), 10):
                    batch = transaction_data[i:i+10]
                    db_cursor.executemany(
                        """
                        INSERT INTO transactions (tx_id, block_number, data)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (tx_id, block_number) DO UPDATE SET data = EXCLUDED.data
                        """,
                        batch
                    )
                    db_conn.commit()
                    
                    logger.info(f"Successfully inserted {len(batch)} transactions for block {block_num}")
            return True
        except (Exception, JSONRPCException) as e:
            logger.error(f"Error processing block {block_num}: {e}")
            retries -= 1
            if retries == 0:
                return False


if __name__ == "__main__":
    process_block_transactions_rpc(123123,db_conn=db_conn, rpc_connection=rpc_connection, logger=logger)
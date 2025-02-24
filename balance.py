import psycopg2
from datetime import datetime
from utils import public_key_to_address
from psycopg2.extras import execute_values
from utils import get_logger

logger = get_logger("balance")

def create_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="bitcoin",
            user="abc",
            password="12345",
            host="localhost",
            port=3004
        )
        logger.info("Database connection established successfully.")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise

def get_vout_address(vout_element):
    try:
        script_type = vout_element['scriptPubKey']['type']
        if script_type == "pubkey":
            return public_key_to_address(vout_element['scriptPubKey']['asm'].split(' ')[0])
        elif script_type in ["pubkeyhash", "scripthash", "witness_v0_keyhash", "witness_v0_scripthash", "witness_v1_taproot"]:
            return vout_element['scriptPubKey']['address']
        else:
            raise ValueError(f"Unknown vout format {vout_element}")
    except KeyError as e:
        logger.error(f"Key error in vout_element: {e}")
        raise

def get_raw_transaction(tx_id, db_cursor):
    try:
        db_cursor.execute("SELECT block_number, data FROM transactions WHERE tx_id = %s", (tx_id,))
        result = db_cursor.fetchone()
        if result is None:
            raise ValueError(f"Transaction with tx_id {tx_id} not found")
        block_number, raw_tx = result[0], result[1]
        logger.debug(f"Retrieved raw transaction for tx_id {tx_id}.")
        return block_number, raw_tx
    except psycopg2.Error as e:
        logger.error(f"Database query error: {e}")
        raise

def get_balance(txid, db_cursor):
    records = []
    try:
        block_number, raw_tx = get_raw_transaction(txid, db_cursor)
        blocktime = raw_tx['blocktime'] # todo: what do we do when tx is None?
        timestamp = datetime.fromtimestamp(blocktime)
    except Exception as e:
        logger.error(f"Failed to get raw transaction {txid}: {e}")
        raise
    
    try:
        senders = []
        for s in raw_tx['vin']:
            if 'coinbase' in s and s['coinbase']:
                senders.append('coinbase')
            elif 'txid' in s and s['txid']:
                prev_block_number, prev_tx = get_raw_transaction(s['txid'], db_cursor)
                prev_n = s['vout']
                prev_out = prev_tx["vout"][prev_n]
                prev_address = get_vout_address(prev_out)
                prev_amount = float(prev_out['value'])
                records.append((timestamp, prev_address, -prev_amount, txid, block_number))
            else:
                raise ValueError(f"Unknown vin format {s}")
        for r in raw_tx['vout']:
            address = get_vout_address(r)
            amount = float(r['value'])
            records.append((timestamp, address, amount, txid, block_number))
        logger.debug(f"Processed balance for transaction {txid}.")
        return records
    except Exception as e:
        logger.error(f"Error processing transaction in block {block_number}, tx {txid}: {e}")
        logger.error(f"Transaction id: {txid}")
        raise

def update_balances(block_number, db_conn):
    db_cursor = db_conn.cursor()
    try:
        logger.info(f"Starting balance update from block number {block_number}.")

        # Get transactions from transactions table for each block_number
        db_cursor.execute("SELECT tx_id FROM transactions WHERE block_number = %s", (block_number,))
        transactions = db_cursor.fetchall()
        if not transactions:
            logger.warning(f"Transaction data from {block_number} is not yet ready.")
            return False

        # For each transaction, get balance and write to balance table
        for (tx_id,) in transactions:
            try:
                records = get_balance(tx_id, db_cursor)
                execute_values(db_cursor, 
                               "INSERT INTO balances (timestamp, address, amount, txid, block_number) VALUES %s ON CONFLICT DO NOTHING",
                               records)
                logger.info(f"Balances updated for transaction {tx_id} from block {block_number}.")
            except Exception as e:
                logger.error(f"Error processing transaction {tx_id}: {e}")
                db_cursor.execute("INSERT INTO unprocessed_transactions (tx_id) VALUES (%s) ON CONFLICT DO NOTHING", (tx_id,))

        # Commit the transaction
        db_conn.commit()
        logger.info("Balance update committed successfully.")
        return True

    except Exception as e:
        logger.error(f"Error updating balances: {e}")
        raise
    finally:
        db_cursor.close()
        logger.info("Database cursor closed.")

if __name__ == '__main__':
    db_conn = None
    try:
        db_conn = create_db_connection()
        db_cursor = db_conn.cursor()
        db_cursor.execute("SELECT MAX(block_number) FROM balances")
        highest_block_number = db_cursor.fetchone()[0]

        if highest_block_number is None:
            highest_block_number = 1

        # Use highest block_number - 1 as starting block
        starting_block = max(highest_block_number - 1, 1)

        while True:
            if not update_balances(starting_block, db_conn):
                break
            starting_block += 1
            
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        if db_conn:
            db_conn.close()
            logger.info("Database connection closed in main.")

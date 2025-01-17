# todo: we need to calc the number of holders that can tolerate 20% downturn - they are believers. - the only metric that matters 
# holding longer than 1 year and experienced 20% downturn

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
import os
import json
import psycopg2
from datetime import datetime
from utils import public_key_to_address
import logging
from logging.handlers import RotatingFileHandler

# Set up logging
log_file = '/tmp/bitcoin_reader.log'
logger = logging.getLogger('bitcoin_tx')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(log_file, maxBytes=1024*1024*1024, backupCount=1) # 1GB max
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Bitcoin data directory (adjust this path for your Mac setup)
bitcoin_datadir = os.path.expanduser("~/Library/Application Support/Bitcoin/")

# RPC connection details
rpc_host = "127.0.0.1"
rpc_port = 8332  # Use 18332 for testnet

# Create an RPC connection using cookie authentication
cookie_file = os.path.join(bitcoin_datadir, ".cookie")
with open(cookie_file, 'r') as f:
    auth_cookie = f.read().strip()

rpc_connection = AuthServiceProxy(f"http://{auth_cookie}@{rpc_host}:{rpc_port}")

# Database connection
db_conn = psycopg2.connect(
    dbname="bitcoin",
    user="abc",
    password="12345",
    host="localhost"
)
db_cursor = db_conn.cursor()

def get_vout_address(vout_element):
    if vout_element['scriptPubKey']['type'] == "pubkey":
        return public_key_to_address(vout_element['scriptPubKey']['asm'].split(' ')[0])
    elif vout_element['scriptPubKey']['type'] == "pubkeyhash":
        return vout_element['scriptPubKey']['address']
    elif vout_element['scriptPubKey']['type'] == "scripthash":
        return vout_element['scriptPubKey']['address']
    elif vout_element['scriptPubKey']['type'] == "witness_v0_keyhash":
        return vout_element['scriptPubKey']['address']
    elif vout_element['scriptPubKey']['type'] == "witness_v0_scripthash":
        return vout_element['scriptPubKey']['address']
    elif vout_element['scriptPubKey']['type'] == "witness_v1_taproot":
        return vout_element['scriptPubKey']['address']
    # elif vout_element['scriptPubKey']['type'] == "multisig":
    #     return vout_element['scriptPubKey']['addresses'][0] if 'addresses' in vout_element['scriptPubKey'] else None
    else:
        raise Exception(f"unknown vout format {vout_element}") 

def get_raw_tx(txid, blockhash, block_number):
    try:
        return rpc_connection.getrawtransaction(txid, True, blockhash)
    except Exception as e:
        logger.error(f"Failed to get raw transaction {txid} from block {blockhash}: {e}")
        raise e

def get_transaction_details(txid, blockhash, block_number):
    records = []
    try:
        raw_tx = rpc_connection.getrawtransaction(txid, True, blockhash)
        blocktime = raw_tx['blocktime']
        timestamp = datetime.fromtimestamp(blocktime)
    except Exception as e:
        logger.error(f"Failed to get raw transaction {txid} from block {blockhash}: {e}")
        raise e
    
    try:
        
        senders = []
        for s in raw_tx['vin']:
            if 'coinbase' in s and s['coinbase']:
                senders.append('coinbase')

            elif 'txid' in s and s['txid']:
                prev_tx = rpc_connection.getrawtransaction(s['txid'], True)
                prev_n = s['vout']
                prev_out = prev_tx["vout"][prev_n]
                prev_address = get_vout_address(prev_out) # this could fail
                prev_amount = prev_out['value']
                records.append((timestamp, prev_address, -prev_amount, txid, block_number))
            else:
                raise Exception(f"unknown vin format {s}") 
            
        receivers = []
        for r in raw_tx['vout']:
            address = get_vout_address(r)
            amount = r['value']
            records.append( (timestamp, address, amount, txid, block_number) )
        return records
    
    except Exception as e:
        logger.error(f"Error processing transaction in block {block_number}, tx {txid}: {e}")
        logger.error(f"transaction id: {txid}")
        raise e

def process_block(block_num):
    try:
        block_hash = rpc_connection.getblockhash(block_num)
        block = rpc_connection.getblock(block_hash)
        block_time = datetime.fromtimestamp(block['time'])
        logger.info(f"Processing block {block_num} at time {block_time}")
        
        # Process all transactions in the block
        records_to_add = []
        if block['tx']:
            for tx in block['tx']:
                try:
                    records = get_transaction_details(tx, block_hash, block_num)
                    records_to_add.extend(records)
                except Exception as e:
                    logger.error(f"Failed to process transaction {tx} in block {block_num}: {e}")
                    
                    raw_tx = get_raw_tx(tx, block_hash, block_num)
                    db_cursor.execute("INSERT INTO unprocessed_transactions (tx, raw_tx, blocktime, blockhash, block_number) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (tx) DO NOTHING;", (tx, json.dumps(raw_tx, default=str), datetime.fromtimestamp(raw_tx['blocktime']), block_hash, block_num))
                    db_conn.commit()

        db_cursor.executemany("INSERT INTO transactions (timestamp, address, amount, tx, block_number) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (timestamp, address, amount, tx, block_number) DO NOTHING;", records_to_add)
        db_conn.commit()
        if records_to_add:
            logger.info(f"Successfully prepared {len(records_to_add)} records for insertion into transactions table.")
        else:
            logger.info("No records to insert into transactions table.")

        return True  # Indicate successful processing

    except Exception as e:
        if "Block height out of range" in str(e):
            print(f"Reached the end of the blockchain at block {block_num}")
            return False  # Indicate end of blockchain
        else:
            logger.error(f"RPC error processing block {block_num}: {e}")
            return False  # Indicate failure

if __name__ == '__main__':
    process_block(336197)

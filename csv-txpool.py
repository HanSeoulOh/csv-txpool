import pandas as pd
from web3 import Web3
import csv
from dotenv import load_dotenv
from dotenv import dotenv_values

import time

config = dotenv_values(".env")
# Connect to the Ethereum node
w3 = Web3(Web3.WebsocketProvider(config['WEBSOCKET_PROVIDER']))

# Check if the connection is successful
if w3.isConnected():
    print("Connected to Reth node")
else:
    print("Failed to connect to Reth node")

# Define the CSV file path
csv_file_path = config['CSV_FILE_PATH']

# Initialize the CSV file with headers
with open(csv_file_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['tx_hash', 'from_address', 'to_address', 'value', 'gas_price', 'gas_limit', 'nonce', 'input_data', 'timestamp'])

# Function to handle and store transactions
def handle_transaction(tx_hash):
    try:
        tx = w3.eth.getTransaction(tx_hash)
        if tx:
            tx_data = {
                'tx_hash': tx['hash'].hex(),
                'from_address': tx['from'],
                'to_address': tx['to'] if tx['to'] else 'Contract Creation',
                'value': tx['value'],
                'gas_price': tx['gasPrice'],
                'gas_limit': tx['gas'],
                'nonce': tx['nonce'],
                'input_data': tx['input'],
                'timestamp': pd.Timestamp.now()
            }
            with open(csv_file_path, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(tx_data.values())
    except Exception as e:
        print(f"Error processing transaction {tx_hash}: {e}")

# Subscribe to pending transactions and process them
subscription = w3.eth.subscribe('pendingTransactions', lambda tx_hash: handle_transaction(tx_hash))

# Keep the script running
while True:
    timerInterval = 5
    print(f"Waiting {timerInterval} seconds for new transaction snapshot...")
    latest_block = w3.eth.blockNumber
    print(f"Latest block number: {latest_block}")
    time.sleep(timerInterval)
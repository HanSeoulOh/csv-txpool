import pandas as pd
from web3 import AsyncWeb3
from web3.providers import WebsocketProviderV2
import csv
from dotenv import dotenv_values
import asyncio
import json
import requests
from websockets import connect

import time

config = dotenv_values(".env")
# Connect to the Ethereum node
w3 = AsyncWeb3(WebsocketProviderV2(config['WEBSOCKET_PROVIDER']))

# Check if the connection is successful
if w3.is_connected():
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

# # Subscribe to pending transactions and process them
# subscription = w3.eth.subscribe('pendingTransactions', lambda tx_hash: handle_transaction(tx_hash))
async def get_event():
    async with w3:
        newpt_subscription_id = await w3.eth.subscribe("newPendingTransactions")
        newhead_subscription_id = await w3.eth.subscribe("newHeads")
        
        latest_block = await w3.eth.get_block("latest")
        print(f"Latest block: {latest_block}")

        async for message in w3.socket.process_subscriptions():
            print(message)

        # while True:
        #     try:
        #         message = await asyncio.wait_for(w3.recv(), timeout=5)
        #         response = json.loads(message)
        #         txHash = response['params']['result']
        #         handle_transaction(txHash)
        #         pass
        #     except Exception as e:
        #         print(f"Error: {e}")
        #         pass

# Keep the script running
timerInterval = 5


if __name__ == "__main__":
    asyncio.run(get_event())
    while True:
        print(f"Waiting {timerInterval} seconds...")
        time.sleep(timerInterval)